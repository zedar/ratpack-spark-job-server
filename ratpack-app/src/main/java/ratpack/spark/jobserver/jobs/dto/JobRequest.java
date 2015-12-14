package ratpack.spark.jobserver.jobs.dto;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Enums;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;
import ratpack.spark.jobserver.jobs.model.JobExecMode;
import ratpack.spark.jobserver.jobs.model.JobParam;

import java.util.List;
import java.util.Objects;

/**
 * Set of parameters defining a job to be executed.
 * This is immutable data transfer object.
 */
@Getter
@ToString
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class JobRequest {
  /**
   * Job execution mode.
   *
   * @return The job execution mode.
   */
  private final JobExecMode mode;

  /**
   * Job unique code name. Maps directly to job's main class name.
   *
   * @return The job code name.
   */
  private final String codeName;

  /**
   * Job execution paramters.
   *
   * @return The job execution parameters
   */
  private final List<JobParam> params;

  /**
   * Creates a job request data transfer object.
   *
   * @param mode an execution mode for the job
   * @param codeName a code name of the job to be executed. Maps directly to registered job classname.
   * @return the {@code JobRequest} instance
   */
  @JsonCreator
  public static JobRequest of(
      @JsonProperty("mode") String mode,
      @JsonProperty("codeName") String codeName,
      @JsonProperty("params") List<JobParam> params) {
    Objects.requireNonNull(mode, "mode parameter is required");
    Objects.requireNonNull(codeName, "codeName parameter is required");
    JobRequest jobRequest = new JobRequest(
      Enums.getIfPresent(JobExecMode.class, mode).or(JobExecMode.ASYNC),
      codeName,
      params);
    return jobRequest;
  }
}
