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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ratpack.server.Service;
import ratpack.server.StartEvent;
import ratpack.server.StopEvent;

import javax.inject.Inject;

/**
 * The service paritipating in the application lifecycle manageing containers resources
 */
public class ContainersLifecycle implements Service {
  private static final Logger LOGGER = LoggerFactory.getLogger(ContainersLifecycle.class);

  private final ContainersService containersService;

  @Inject
  public ContainersLifecycle(final ContainersService containersService) {
    this.containersService = containersService;
  }

  @Override
  public void onStart(StartEvent event) throws Exception {
    LOGGER.info("STARTED");
  }

  @Override
  public void onStop(StopEvent event) throws Exception {
    LOGGER.info("STOPPING");
    containersService.onStop();
  }
}
