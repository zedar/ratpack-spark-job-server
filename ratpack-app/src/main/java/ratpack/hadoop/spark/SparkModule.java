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

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import ratpack.hadoop.spark.containers.ContainersService;
import ratpack.hadoop.spark.func.movierecommendation.MovieRecommendationService;
import ratpack.hadoop.spark.func.topn.TopNService;

import javax.inject.Singleton;

/**
 * Provides configuration for Apache Spark and Hadoop file system.
 */
public class SparkModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(SparkEndpoints.class).in(Scopes.SINGLETON);
  }

  /**
   * Provides {@link TopNService} executing job for extracting topN active users with {@code Apache Spark}.
   *
   * @param config HDFS and Apache Spark configuration settings
   * @param sparkJobsConfig a configuration of jobs
   * @param containersService a service for job containers
   * @return the singleton instance of the {@link TopNService} implementation
   */
  @Provides
  @Singleton
  public TopNService topNService(final SparkConfig config, final SparkJobsConfig sparkJobsConfig, final ContainersService containersService) {
    return new TopNService(config, sparkJobsConfig, containersService);
  }

  /**
   * Provides {@link MovieRecommendationService} executing movie recommendation algorithm with {@code Apache Spark}
   *
   * @param config HDFS and Apache Spark configuration settings
   * @param sparkJobsConfig a configuration of jobs
   * @param containersService a service for job containers
   * @return the signleton instance of the {@link MovieRecommendationService}
   */
  @Provides
  @Singleton
  public MovieRecommendationService movieRecommendationService(final SparkConfig config, final SparkJobsConfig sparkJobsConfig, final ContainersService containersService) {
    return new MovieRecommendationService(config, sparkJobsConfig, containersService);
  }
}
