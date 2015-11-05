package ratpack.spark.jobserver.jobs.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import lombok.*;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Job executions values
 */
@ToString
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Job {
  /**
   * Unique job identifier.
   *
   * @return job's unique identifier
   */
  @Getter
  private String jobId;

  /**
   * Job execution status
   *
   * @return job's execution status
   */
  @Getter
  private JobExecStatus jobStatus;

  /**
   * List of job output values/parameters.
   *
   * @return the list of job output values/parameters
   */
  @Getter
  private List<JobValue> values;

  /**
   * Sets job jobId
   * @param jobId an identifier of a job
   * @return this
   */
  public Job jobId(final String jobId) {
    this.jobId = jobId;
    return this;
  }

  /**
   * Sets job unique identifier as {@code UUID}
   * @param uuid globally unique identifier
   * @return this
   */
  public Job jobId(final UUID uuid) {
    this.jobId = uuid.toString();
    return this;
  }

  /**
   * Sets job's execution status.
   * @param jobStatus new job status
   * @return this
   */
  public Job jobStatus(JobExecStatus jobStatus) {
    this.jobStatus = jobStatus;
    return this;
  }

  /**
   * Adds new pair name/value to the list of values.
   * @param value a single job value entry
   * @return this
   */
  public Job addValue(final JobValue value) {
    if (values == null) {
      values = Lists.newArrayList();
    }
    values.add(value);
    return this;
  }

  /**
   * Adds new pair name/value to the list of values
   * @param value a single job value entry
   * @return this
   */
  public Job addValue(final List<String> value) {
    return addValue(JobValue.of(value));
  }

  /**
   * Factory method for job result without job values
   * @param uuid unique identifier of the job
   * @return job values
   */
  public static Job of(final UUID uuid, final JobExecStatus status) {
    return new Job(uuid.toString(), status, null);
  }

  /**
   * Factory method for job result with list of values.
   * @param uuid unique identifier of the job
   * @param values list of pairs of name/value
   * @return job values
   */
  public static Job of(final UUID uuid, final JobExecStatus status, final List<JobValue> values) {
    return new Job(uuid.toString(), status, values);
  }

  /**
   * Job value record.
   */
  @AllArgsConstructor(access = AccessLevel.PRIVATE)
  public static class JobValue {
    @Getter @Setter
    private List<String> values;

    /**
     * Factory method for single job value entry.
     * @param values list of values in the form of string
     * @return the single job value
     */
    public static JobValue of(final List<String> values) {
      return new JobValue(values);
    }

    /**
     * Converts list of list of singular values to list of values
     * @param values list of list of singular values
     * @return the list of values
     */
    public static List<JobValue> to(final List<List<String>> values) {
      if (values == null) {
        return ImmutableList.of();
      }
      List<JobValue> outValues = values
        .stream()
        .map(v -> JobValue.of(v))
        .collect(Collectors.toList());
      return ImmutableList.copyOf(outValues);
    }
  }
}
