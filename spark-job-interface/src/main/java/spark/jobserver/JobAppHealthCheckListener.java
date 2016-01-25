package spark.jobserver;

import org.apache.spark.scheduler.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Job Application lifecycle and helth check listener.
 */
public class JobAppHealthCheckListener implements SparkListener {
  private static final Logger LOGGER = LoggerFactory.getLogger(JobAppHealthCheckListener.class);

  private AtomicBoolean appStarted = new AtomicBoolean(false);
  private AtomicInteger executorCounter = new AtomicInteger(0);

  /**
   * Is application registered and started?
   * @return true if application is already started
   */
  public Boolean isAppStarted() {
    return appStarted.get();
  }

  /**
   * Get counter of the registered executors.
   * @return the executor counter
   */
  public Integer getExecutorCounter() {
    return executorCounter.get();
  }

  /**
   * Was the connection between Spark Driver and Master healthy?
   * @return {@code true} if connection between Spark Driver and Master is healthy
   */
  public Boolean isHealthy() {
    if (appStarted.get() && executorCounter.get() <= 0) {
      return false;
    } else {
      return true;
    }
  }

  @Override
  public void onApplicationStart(SparkListenerApplicationStart applicationStart) {
    appStarted.compareAndSet(false, true);
    LOGGER.debug("APPSTART [{}]", toString());
  }

  @Override
  public void onJobStart(SparkListenerJobStart jobStart) {
    appStarted.compareAndSet(false, true);
    LOGGER.debug("JOBSTART [{}]", toString());
  }

  @Override
  public void onExecutorAdded(SparkListenerExecutorAdded executorAdded) {
    appStarted.compareAndSet(false, true);
    executorCounter.incrementAndGet();
    LOGGER.debug("EXECUTORADDED [{}]", toString());
  }

  @Override
  public void onExecutorRemoved(SparkListenerExecutorRemoved executorRemoved) {
    executorCounter.decrementAndGet();
    LOGGER.debug("EXECUTORREMOVED [{}]", toString());
  }

  @Override
  public void onJobEnd(SparkListenerJobEnd jobEnd) {
    LOGGER.debug("JOBEND [{}]", toString());
  }

  @Override
  public void onApplicationEnd(SparkListenerApplicationEnd applicationEnd) {
    executorCounter.set(0);
    LOGGER.debug("APPEND [{}]", toString());
  }

  @Override
  public String toString() {
    return "started: " + appStarted.get() + ", executors: " + executorCounter.get();
  }

  @Override
  public void onStageCompleted(SparkListenerStageCompleted stageCompleted) {
  }

  @Override
  public void onStageSubmitted(SparkListenerStageSubmitted stageSubmitted) {
  }

  @Override
  public void onTaskStart(SparkListenerTaskStart taskStart) {
  }

  @Override
  public void onTaskGettingResult(SparkListenerTaskGettingResult taskGettingResult) {
  }

  @Override
  public void onTaskEnd(SparkListenerTaskEnd taskEnd) {
  }

  @Override
  public void onEnvironmentUpdate(SparkListenerEnvironmentUpdate environmentUpdate) {
  }

  @Override
  public void onBlockManagerAdded(SparkListenerBlockManagerAdded blockManagerAdded) {
  }

  @Override
  public void onBlockManagerRemoved(SparkListenerBlockManagerRemoved blockManagerRemoved) {
  }

  @Override
  public void onUnpersistRDD(SparkListenerUnpersistRDD unpersistRDD) {
  }

  @Override
  public void onExecutorMetricsUpdate(SparkListenerExecutorMetricsUpdate executorMetricsUpdate) {
  }

  @Override
  public void onBlockUpdated(SparkListenerBlockUpdated blockUpdated) {
  }
}
