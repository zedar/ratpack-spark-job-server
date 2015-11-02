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
package ratpack.spark.jobserver;

import com.google.common.collect.Lists;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * The configuration for all registered Apache Spark jobs.
 * Point to directories where jars with algorithm definition can be found.
 */
@NoArgsConstructor
@ToString
public class SparkJobsConfig {
  private List<String> jarPaths;
  private List<String> classNames;

  /**
   * Array of paths (directories or directly jars) containing spark jobs and their algorithms
   * @return the array of paths to jars with Spark job implementation
   */
  public String[] getJarPaths() {
    return (jarPaths == null ? Collections.emptyList() : jarPaths).toArray(new String[]{});
  }

  /**
   * Sets an array of paths containing jars with Spark job implementations.
   * @param jarsPaths a list of paths to jars with Spark job implementations
   * @return this
   */
  public SparkJobsConfig jarsPaths(final List<String> jarsPaths) {
    this.jarPaths = jarsPaths;
    return this;
  }

  /**
   * Arrays of full class names implementing registered algorithms
   * @return the arrays of class names
   */
  public String[] getClassNames() {
    return (classNames == null ? Collections.emptyList() : classNames).toArray(new String[]{});
  }

  /**
   * Sets an array of class names implementing {@code JobApi} interface
   * @param classNames a list of class names implementing {@code JobApi} interface
   * @return this
   */
  public SparkJobsConfig classNames(final List<String> classNames) {
    this.classNames = classNames;
    return this;
  }
}
