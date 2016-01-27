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

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import ratpack.spark.jobserver.SparkConfig;
import ratpack.spark.jobserver.SparkJobsConfig;

/**
 * Provides services for working with containers able to execute Apache Spark jobs.
 */
public class ContainersModule extends AbstractModule {
  @Override
  protected void configure() {
    bind(ContainersService.class).in(Scopes.SINGLETON);
  }

  /**
   * Provides containers lifecycle service with implementation of the {@code onStop} method that cleans up all running containers.
   * @param sparkConfig all major spark config parameters
   * @param sparkJobsConfig a configuration of registered spark jobs
   * @param containersService a service to instantiate jobs containers
   * @return the singleton of {@link ratpack.spark.jobserver.containers.ContainersLifecycle} service
   */
  @Provides
  @Singleton
  public ContainersLifecycle containersLifecycle(final SparkConfig sparkConfig, final SparkJobsConfig sparkJobsConfig, final ContainersService containersService) {
    return new ContainersLifecycle(sparkConfig, sparkJobsConfig, containersService);
  }
}
