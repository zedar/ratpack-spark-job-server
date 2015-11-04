package ratpack.spark.jobserver.jobs.model;

/**
 * List of possible job execution statuses
 */
public enum JobExecStatus {
  /**
   * Job has finished
   */
  FINISHED,

  /**
   * Job is still working
   */
  WORKING,

  /**
   * Job has failed
   */
  FAILED
}
