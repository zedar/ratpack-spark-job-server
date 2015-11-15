/*
 * Copyright 2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ratpack.spark.jobserver.containers;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ratpack.exec.Blocking;
import ratpack.exec.Promise;
import ratpack.spark.jobserver.SparkConfig;
import ratpack.spark.jobserver.SparkJobsConfig;
import ratpack.spark.jobserver.util.ClassUtil;

import javax.inject.Inject;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.rmi.UnexpectedException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;

/**
 * The singleting services providing API for managing containers running Apache Spark jobs
 */
public class ContainersService {
  private static final Logger LOGGER = LoggerFactory.getLogger(ContainersService.class);

  private static final String SPARK_APP_NAME = "JOBSERVER";

  private final SparkConfig config;
  private final SparkJobsConfig sparkJobsConfig;

  private URLClassLoader rootClassLoader;
  private URLClassLoader containersClassLoader;

  private ConcurrentMap<String, Container> containers = Maps.newConcurrentMap();

  private ReentrantLock lock = new ReentrantLock();

  // hadoop configuration shared across all the containers
  private Object hadoopConfiguration;

  // spark configuration used to create JavaSprakContext
  private Object sparkConfig;

  // spark context shared across all the containers. Only one SparkContext can exists per JVM
  private Object javaSparkContext;

  @Inject
  public ContainersService(SparkConfig config, SparkJobsConfig sparkJobsConfig) {
    this.config = config;
    this.sparkJobsConfig = sparkJobsConfig;
  }

  /**
   * Creates container for executing Apache Spark job. Every {@code appName} has own container. Containers are reusable
   * across the same {@code appNames}.
   * @param jobCodeName an job unique code name
   * @param jobClassName job execution class name
   * @return the promise for job container
   */
  public Promise<Container> getJobContainer(String jobCodeName, String jobClassName) {
    Container container = containers.get(jobCodeName);
    if (container != null) {
      return Promise.value(container);
    }
    return Blocking.get(() -> {
      // IMPORTANT: lock is required in order to synchronize initalization of spark context.
      // Spark context for the partivular job can be initialized only once.
      lock.lock();
      // IMPORTANT: many thread can wait for the lock. If spark context was intialized there is not need to initialize it again
      if (containers.get(jobCodeName) != null) {
        return containers.get(jobCodeName);
      }
      ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
      try {
        URLClassLoader containersClassLoader = getContainersClassLoader();
        Thread.currentThread().setContextClassLoader(containersClassLoader);

        Class configurationClass = containersClassLoader.loadClass("org.apache.hadoop.conf.Configuration");
        if (hadoopConfiguration == null) {
          Object configuration = configurationClass.newInstance();
          Method method = configurationClass.getMethod("set", String.class, String.class);
          method.invoke(configuration, "fs.defaultFS", config.getFileSystemAddress());
          method.invoke(configuration, "io.compression.codecs", "com.hadoop.compression.lzo.LzopCodec");
          hadoopConfiguration = configuration;
        }


        Class javaSparkContextClass = containersClassLoader.loadClass("org.apache.spark.api.java.JavaSparkContext");
        if (javaSparkContext == null) {
          // Initialize list of jobs jars
          Map<String, String> sparkContextJars = Maps.newHashMap();
          if (sparkContextJars.isEmpty()) {
            String[] jobClassNames = sparkJobsConfig.getClassNames();
            for (String cn : jobClassNames) {
              String jobJarPath = ClassUtil.findContainingJar(containersClassLoader, cn);
              if (sparkContextJars.get(jobJarPath) == null) {
                sparkContextJars.put(jobJarPath, jobJarPath);
              }
            }
          }
          Class sparkConfClass = containersClassLoader.loadClass("org.apache.spark.SparkConf");

          LOGGER.debug("SPARK CONFIG: {}", config.toString());

          sparkConfig = sparkConfClass.newInstance();
          Method m = sparkConfClass.getMethod("setAppName", String.class);
          m.invoke(sparkConfig, SPARK_APP_NAME);
          m = sparkConfClass.getMethod("setMaster", String.class);
          m.invoke(sparkConfig, config.getMaster());
          if (!Strings.isNullOrEmpty(config.getHomeDir())) {
            m = sparkConfClass.getMethod("setSparkHome", String.class);
            m.invoke(sparkConfig, config.getHomeDir());
          }
          String maxCoresPerTask = config.getMaxCoresPerTask() == null ? "2" : config.getMaxCoresPerTask().toString();
          m = sparkConfClass.getMethod("set", String.class, String.class);
          m.invoke(sparkConfig, "spark.cores.max", maxCoresPerTask);
          m.invoke(sparkConfig, "spark.serializer", "org.apache.spark.serializer.KryoSerializer");
          m.invoke(sparkConfig, "spark.io.compression.codec", "lz4");
          m.invoke(sparkConfig, "spark.driver.allowMultipleContexts", "true");
          m.invoke(sparkConfig, "spark.rpc.askTimeout", "20");
          m.invoke(sparkConfig, "spark.rpc.numRetries", "1");
          //
          //m.invoke(sparkConfig, "spark.executor.memory", "4G");
          //
          if (!Strings.isNullOrEmpty(config.getExtraJavaOptions())) {
            m.invoke(sparkConfig, "spark.executor.extraJavaOptions", config.getExtraJavaOptions());
          }
          //m.invoke(sparkConf, "spark.broadcast.factory", "org.apache.spark.broadcast.HttpBroadcastFactory");
          Method sparkConfSetJarsMethod = sparkConfClass.getMethod("setJars", new Class[]{String[].class});
          sparkConfSetJarsMethod.invoke(sparkConfig, new Object[]{sparkContextJars.keySet().toArray(new String[]{})});

          Constructor jscConstructor = javaSparkContextClass.getDeclaredConstructor(sparkConfClass);
          Object jsCtx = jscConstructor.newInstance(sparkConfig);

          javaSparkContext = jsCtx;
        }

        Class appClass = containersClassLoader.loadClass(jobClassName);
        Method beforeJobMethod = appClass.getMethod("beforeJob", configurationClass, javaSparkContextClass, Map.class);
        Method runJobMethod = appClass.getMethod("runJob", configurationClass, javaSparkContextClass, Map.class);
        Method fetchJobResultsMethod = appClass.getMethod("fetchResults", configurationClass, javaSparkContextClass, Map.class);
        Method afterJobMethod = appClass.getMethod("afterJob", configurationClass, javaSparkContextClass, Map.class);
        Method cleanUpMethod = appClass.getMethod("cleanUp");
        Class jobAPIClass = containersClassLoader.loadClass("spark.jobserver.JobAPI");
        if (jobAPIClass.isAssignableFrom(appClass)) {
          // create instance of the job
          Object job = appClass.newInstance();
          containers.put(jobCodeName, new Container(
            containersClassLoader, hadoopConfiguration, javaSparkContext,
            job, beforeJobMethod, runJobMethod, fetchJobResultsMethod, afterJobMethod, cleanUpMethod));
        } else {
          throw new UnexpectedException("Job does not support JobAPI interface");
        }


      } finally {
        Thread.currentThread().setContextClassLoader(classLoader);
        lock.unlock();
      }
      return containers.get(jobCodeName);
    });
  }

  /**
   * Stop/shutdown all resources used by containers
   * @throws Exception
   */
  public void onStop() throws Exception {
    if (javaSparkContext != null) {
      Class javaSparkContextClass = containersClassLoader.loadClass("org.apache.spark.api.java.JavaSparkContext");
      Method jscStopMethod = javaSparkContextClass.getMethod("stop");
      jscStopMethod.invoke(javaSparkContext);
    }
    if (containers.size() > 0) {
      stopJavaSparkContexts(containers.entrySet().toArray(new Container[]{}));
    }
  }

  private URLClassLoader getRootClassLoader() throws Exception {
    if (rootClassLoader != null) {
      return rootClassLoader;
    }
    // find urls for all jars necessary for Apache Spark
    ArrayList<URL> urlArrayList = new ArrayList<URL>();
    Files.walk(Paths.get(config.getLibsDir())).forEach(p -> {
      if (Files.isRegularFile(p)) {
        try {
          urlArrayList.add(p.toUri().toURL());
        } catch (Exception ex) {
          throw new RuntimeException(ex);
        }
      }
    });
    // create root class loader
    rootClassLoader = URLClassLoader.newInstance(urlArrayList.toArray(new URL[]{}), null);

    return rootClassLoader;
  }

  private URLClassLoader getContainersClassLoader() throws Exception {
    if (containersClassLoader != null) {
      return containersClassLoader;
    }

    List<URL> urlArrayList = Lists.newArrayList();
    String[] jobJarsDirs = sparkJobsConfig.getJarPaths();
    for (String jobJarsDir : jobJarsDirs) {
      Path jobClassPath = Paths.get(jobJarsDir);
      if (Files.isDirectory(jobClassPath)) {
        Files.walk(jobClassPath).forEach(p -> {
          if (Files.isRegularFile(p)) {
            try {
              urlArrayList.add(p.toUri().toURL());
            } catch (Exception ex) {
              throw new RuntimeException(ex);
            }
          }
        });
      } else {
        urlArrayList.add(jobClassPath.toUri().toURL());
      }
    }

    containersClassLoader = URLClassLoader.newInstance(urlArrayList.toArray(new URL[]{}), getRootClassLoader());
    return containersClassLoader;
  }

  private void stopJavaSparkContexts(Container... containers) throws Exception{
    for (Container container : containers) {
      container.stop();
    }
  }
}
