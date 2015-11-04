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

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import ratpack.spark.jobserver.containers.ContainersService;
import ratpack.spark.jobserver.jobs.JobsEndpoints;
import ratpack.spark.jobserver.jobs.JobsService;
import ratpack.spark.jobserver.jobs.model.JobsRepository;

import javax.inject.Singleton;

/**
 * Provides configuration for Apache Spark and Hadoop file system.
 */
public class SparkModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(JobsRepository.class).in(Scopes.SINGLETON);
    bind(JobsEndpoints.class).in(Scopes.SINGLETON);
  }

  /**
   * Provides {@link JobsService}, executing any job in SYNC or ASYNC mode.
   * @param sparkConfig HDFS and Apache Spark configuration settings
   * @param sparkJobsConfig a configuration of jobs
   * @param containersService a service for job containers
   * @return the singleton instance of the {@link JobsService} implementation
   */
  @Provides
  @Singleton
  public JobsService jobsService(final SparkConfig sparkConfig, final SparkJobsConfig sparkJobsConfig, final ContainersService containersService, final JobsRepository jobsRepository) {
    return new JobsService(sparkConfig, sparkJobsConfig, containersService, jobsRepository);
  }
}
