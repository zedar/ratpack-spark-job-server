package ratpack.hadoop.spark.containers;

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
    LOGGER.debug("STARTED");
  }

  @Override
  public void onStop(StopEvent event) throws Exception {
    LOGGER.debug("STOPPED");
    containersService.onStop();
  }
}
