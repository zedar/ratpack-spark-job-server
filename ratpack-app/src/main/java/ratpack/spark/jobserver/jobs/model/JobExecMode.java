package ratpack.spark.jobserver.jobs.model;

/**
 * List of available job execution modes.
 */
public enum JobExecMode {
  /**
   * Execute a job in request&response mode. Wait for the job result.
   */
  SYNC,

  /**
   * Execute a job in fire&ask later mode. Do not wait for the job result.
   */
  ASYNC;
}
