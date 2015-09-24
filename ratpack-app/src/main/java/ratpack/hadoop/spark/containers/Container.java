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
package ratpack.hadoop.spark.containers;

import com.google.common.collect.ImmutableMap;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;
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
   * The method {@code runJob} used to execute the job
   */
  private Method runJobMethod;

  /**
   * The method {@code fetchJobResults} used to fetch job execution results.
   */
  private Method fetchJobResultsMethod;

  /**
   * Stops container resources: Java Spark Context
   * @throws Exception
   */
  public void stop() throws Exception {
  }

  /**
   * Run the job for the given parameters
   * @param inputPath a path to HDFS/filesystem that contain input data
   * @param outputPath a path to HDFS/filesystem that should contain output data postfixed with job's uuid
   * @return the promise for job's uuid
   * @throws Exception any
   */
  public Promise<String> runJob(ImmutableMap<String, String> params, String inputPath, String outputPath) {
    return Blocking.get(() -> {
      String uuid = UUID.randomUUID().toString();

      ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
      try {
        Thread.currentThread().setContextClassLoader(jobClassLoader);
        runJobMethod.invoke(null, hadoopConfiguration, javaSparkContext, params, inputPath, outputPath + "-" + uuid);
      } finally {
        Thread.currentThread().setContextClassLoader(classLoader);
      }

      return uuid;
    });
  }

  /**
   * Fetch results of job execution in the form of {@code T} type.
   * @param outputPath the path to look for the results. Either HDFS or filesystem path posfixed with uuid
   * @param uuid a unique id of the job
   * @param <T> a data type containing job results
   * @return the promise for the job results
   * @throws Exception any
   */
  public <T> Promise<T> fetchJobResults(String outputPath, String uuid) {
    return Blocking.get(() -> {
      ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
      try {
        Thread.currentThread().setContextClassLoader(jobClassLoader);
        return (T)fetchJobResultsMethod.invoke(null, hadoopConfiguration, outputPath+ "-" + uuid);
      } finally {
        Thread.currentThread().setContextClassLoader(classLoader);
      }
    });
  }
}
