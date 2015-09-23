package ratpack.hadoop.spark.containers;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import ratpack.exec.Blocking;
import ratpack.exec.Promise;
import ratpack.hadoop.spark.SparkConfig;
import ratpack.hadoop.spark.util.ClassUtil;

import javax.inject.Inject;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The singleting services providing API for managing containers running Apache Spark jobs
 */
public class ContainersService {
  private final SparkConfig config;

  private URLClassLoader rootClassLoader;

  private ConcurrentMap<String, Container> containers = Maps.newConcurrentMap();

  private ReentrantLock lock = new ReentrantLock();

  @Inject
  public ContainersService(SparkConfig config) {
    this.config = config;
  }

  /**
   * Creates container for executing Apache Spark job. Every {@code appName} has own container. Containers are reusable
   * across the same {@code appNames}.
   * @param jobName an job name
   * @param jobClassJarPath a job class path, where job's jar is present
   * @param jobClassName job execution class name
   * @return the promise for job container
   */
  public Promise<Container> getJobContainer(String jobName, String jobClassJarPath, String jobClassName) {
    Container container = containers.get(jobName);
    if (container != null) {
      return Promise.value(container);
    }
    return Blocking.get(() -> {
      // IMPORTANT: lock is required in order to synchronize initalization of spark context.
      // Spark context for the partivular job can be initialized only once.
      lock.lock();
      // IMPORTANT: many thread can wait for the lock. If spark context was intialized there is not need to initialize it again
      if (containers.get(jobName) != null) {
        return containers.get(jobName);
      }
      ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
      try {
        List<URL> urlArrayList = Lists.newArrayList();
        Path jobClassPath = Paths.get(jobClassJarPath);
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
        URLClassLoader jobClassLoader = URLClassLoader.newInstance(urlArrayList.toArray(new URL[]{}), getRootClassLoader());
        Thread.currentThread().setContextClassLoader(jobClassLoader);

        // find path to Apache Spark app jar
        String appJarPath = ClassUtil.findContainingJar(jobClassLoader, jobClassName);

        Class configurationClass = jobClassLoader.loadClass("org.apache.hadoop.conf.Configuration");
        Object configuration = configurationClass.newInstance();
        Method method = configurationClass.getMethod("set", String.class, String.class);
        method.invoke(configuration, "fs.defaultFS", config.getFileSystemAddress());

        Class sparkConfClass = jobClassLoader.loadClass("org.apache.spark.SparkConf");
        Object sparkConf = sparkConfClass.newInstance();
        Method m = sparkConfClass.getMethod("setAppName", String.class);
        m.invoke(sparkConf, jobName);
        m = sparkConfClass.getMethod("setMaster", String.class);
        m.invoke(sparkConf, config.getMaster());
        m = sparkConfClass.getMethod("setJars", new Class[]{String[].class});
        m.invoke(sparkConf, new Object[]{new String[]{appJarPath}});
        String maxCoresPerTask = config.getMaxCoresPerTask() == null ? "2" : config.getMaxCoresPerTask().toString();
        m = sparkConfClass.getMethod("set", String.class, String.class);
        m.invoke(sparkConf, "spark.cores.max", maxCoresPerTask);
        m.invoke(sparkConf, "spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        m.invoke(sparkConf, "spark.io.compression.codec", "lz4");

        Class javaSparkContextClass = jobClassLoader.loadClass("org.apache.spark.api.java.JavaSparkContext");
        Constructor jscConstructor = javaSparkContextClass.getDeclaredConstructor(sparkConfClass);
        Object javaSparkContext = jscConstructor.newInstance(sparkConf);
        Method jscStopMethod = javaSparkContextClass.getMethod("stop");

        Class appClass = jobClassLoader.loadClass(jobClassName);
        Method runJobMethod = appClass.getMethod("runJob", configurationClass, javaSparkContextClass, Map.class, String.class, String.class);

        Method fetchJobResultsMethod = appClass.getMethod("fetchJobResults", configurationClass, String.class);

        containers.put(jobName, new Container(jobClassLoader, configuration, javaSparkContext, jscStopMethod, runJobMethod, fetchJobResultsMethod));

      } finally {
        Thread.currentThread().setContextClassLoader(classLoader);
        lock.unlock();
      }
      return containers.get(jobName);
    });
  }

  /**
   * Stop/shutdown all resources used by containers
   * @throws Exception
   */
  public void onStop() throws Exception {
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

  private void stopJavaSparkContexts(Container... containers) throws Exception{
    for (Container container : containers) {
      if (container.getJavaSparkContext() != null) {
        container.getJavaSparkContextStopMethod().invoke(container.getJavaSparkContext());
      }
    }
  }
}
