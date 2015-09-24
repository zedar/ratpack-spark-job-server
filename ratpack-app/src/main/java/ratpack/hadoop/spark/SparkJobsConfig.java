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
package ratpack.hadoop.spark;

import com.google.common.collect.Lists;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.Objects;

/**
 * The configuration for all registered Apache Spark jobs.
 * Point to directories where jars with algorithm definition can be found.
 */
@NoArgsConstructor
@ToString
public class SparkJobsConfig {
  private String topNJarsDir;
  private String topNClassName;
  private String movieRecommendationJarsDir;
  private String movieRecommendationClassName;

  /**
   * The path to folder with jars containing TopN algorithm. In Apache Spark convention
   * @return the path to TopN jars
   */
  public String getTopNJarsDir() {
    return topNJarsDir;
  }

  /**
   * Sets the path to TopN jars folder.
   * @param topNJars the path
   * @return this
   */
  public SparkJobsConfig topNJarsDir(String topNJars) {
    this.topNJarsDir = topNJars;
    return this;
  }

  /**
   * The class name of the TopN algorithm.
   * @return full class name
   */
  public String getTopNClassName() {
    return topNClassName;
  }

  /**
   * Sets the class name for TopN algorithm
   * @param topNClassName full class name
   * @return this
   */
  public SparkJobsConfig topNClassName(String topNClassName) {
    this.topNClassName = topNClassName;
    return this;
  }

  /**
   * The path to folder with jars or one jar implementing movie recommendation algorithm.
   * @return tge path to jars
   */
  public String getMovieRecommendationJarsDir() {
    return movieRecommendationJarsDir;
  }

  /**
   * Sets the path to the jars implementing movie recommendations algorithm
   * @param movieRecommendationJarsDir a path
   * @return this
   */
  public SparkJobsConfig movieRecommendationJarsDir(String movieRecommendationJarsDir) {
    this.movieRecommendationJarsDir = movieRecommendationJarsDir;
    return this;
  }

  /**
   * The class name for movie recommendation algorithm.
   * @return full class name
   */
  public String getMovieRecommendationClassName() {
    return movieRecommendationClassName;
  }

  /**
   * Sets the class name for movie recommendation algorithm
   * @param movieRecommendationClassName full class name
   * @return this
   */
  public SparkJobsConfig movieRecommendationClassName(String movieRecommendationClassName) {
    this.movieRecommendationClassName = movieRecommendationClassName;
    return this;
  }

  /**
   * Array of directories with jars needed for executing all of the registered algorithms
   * @return the array of dirs with jars
   */
  public String[] getJarsDirs() {
    Objects.requireNonNull(topNJarsDir);
    Objects.requireNonNull(movieRecommendationJarsDir);
    return Lists.newArrayList(topNJarsDir, movieRecommendationJarsDir).toArray(new String[]{});
  }

  /**
   * Arrays of full class names implementing registered algorithms
   * @return the arrays of class names
   */
  public String[] getClassNames() {
    Objects.requireNonNull(topNClassName);
    Objects.requireNonNull(movieRecommendationClassName);
    return Lists.newArrayList(topNClassName, movieRecommendationClassName).toArray(new String[]{});
  }
}
