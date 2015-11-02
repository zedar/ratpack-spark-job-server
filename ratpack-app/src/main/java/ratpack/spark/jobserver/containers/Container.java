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

import com.google.common.collect.ImmutableMap;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ratpack.exec.Blocking;
import ratpack.exec.Promise;

import java.lang.reflect.Method;
import java.net.URLClassLoader;
import java.util.UUID;

/**
 * Container holds class loader, java spark context
 */
@ToString
@AllArgsConstructor
public class Container {
  private static final Logger LOGGER = LoggerFactory.getLogger(Container.class);

  /**
   * Class loader with jars containing Apache Spark Job jars as well as all dependencies needed for Apache Spark.
   *
   * @return the class loader to used by Apache Spark Job
   */
  @Getter private URLClassLoader jobClassLoader;

  /**
   * Hadoop {@code Configuration} used for HDFS operations.
   *
   * @return Hadoop HDFS configuration
   */
  @Getter private Object hadoopConfiguration;

  /**
   * Java Spark Context object, configured to execute Spark job.
   *
   * @return the object with Java Spark Context
   */
  @Getter private Object javaSparkContext;

  /**
   * The job instance implementing {@code  JobAPI} interface.
   */
  private Object job;

  /**
   * The method used to prepare job data, like training the model.
   */
  private Method beforeJobMethod;

  /**
   * The method {@code runJob} used to execute the job
   */
  private Method runJobMethod;

  /**
   * The method {@code fetchJobResults} used to fetch job execution results.
   */
  private Method fetchJobResultsMethod;

  /**
   * The method {@code afterJob} used to clean temporary job results
   */
  private Method afterJobMethod;

  /**
   * The method used to clean all data allocated and referenced by the job.
   */
  private Method cleanUpMethod;

  /**
   * Stops container resources: Java Spark Context
   * @throws Exception
   */
  public void stop() throws Exception {
    if (cleanUpMethod != null) {
      ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
      try {
        Thread.currentThread().setContextClassLoader(jobClassLoader);
        cleanUpMethod.invoke(job);
      } finally {
        Thread.currentThread().setContextClassLoader(classLoader);
      }
    }
  }

  /**
   * Run the job for the given parameters
   * @param params map of job parameters
   * @return the promise for job's uuid
   * @throws Exception any
   */
  public Promise<Void> runJob(ImmutableMap<String, String> params) {
    return Blocking.get(() -> {
      ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
      try {
        Thread.currentThread().setContextClassLoader(jobClassLoader);
        if (beforeJobMethod != null) {
          beforeJobMethod.invoke(job, hadoopConfiguration, javaSparkContext, params);
        }
        runJobMethod.invoke(job, hadoopConfiguration, javaSparkContext, params);
      } catch (Exception ex) {
        LOGGER.debug("EXCEPTION FROM SPARK: " + ex.getMessage());
        throw ex;
      }
      finally {
        Thread.currentThread().setContextClassLoader(classLoader);
      }
      return null;
    });
  }

  /**
   * Fetch results of job execution in the form of {@code T} type.
   * @param params list of job parameters
   * @param <T> a data type containing job results
   * @return the promise for the job results
   * @throws Exception any
   */
  public <T> Promise<T> fetchJobResults(ImmutableMap<String, String> params) {
    return Blocking.get(() -> {
      ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
      try {
        Thread.currentThread().setContextClassLoader(jobClassLoader);
        T result = (T)fetchJobResultsMethod.invoke(job, hadoopConfiguration, javaSparkContext, params);
        if (afterJobMethod != null) {
          afterJobMethod.invoke(job, hadoopConfiguration, javaSparkContext, params);
        }
        return result;
      } finally {
        Thread.currentThread().setContextClassLoader(classLoader);
      }
    });
  }
}
