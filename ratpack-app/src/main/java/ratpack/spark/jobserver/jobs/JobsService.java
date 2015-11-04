package ratpack.spark.jobserver.jobs;

import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ratpack.exec.Execution;
import ratpack.exec.Promise;
import ratpack.exec.Result;
import ratpack.spark.jobserver.SparkConfig;
import ratpack.spark.jobserver.SparkJobsConfig;
import ratpack.spark.jobserver.containers.ContainersService;
import ratpack.spark.jobserver.jobs.dto.JobRequest;
import ratpack.spark.jobserver.jobs.model.JobExecMode;
import ratpack.spark.jobserver.jobs.model.JobExecStatus;
import ratpack.spark.jobserver.jobs.model.JobParam;
import ratpack.spark.jobserver.jobs.model.JobValues;

import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * Executes Apache Spark jobs. Returns either job result (if executed in sync mode) or just result with reference job jobId (if executed in async mode).
 */
public class JobsService {
  private static final Logger LOGGER = LoggerFactory.getLogger(JobsService.class);

  private final SparkConfig sparkConfig;
  private final SparkJobsConfig sparkJobsConfig;
  private final ContainersService containersService;

  /**
   * The constructor
   * @param sparkConfig Spark configuration defined in {@code application.properties} file
   * @param sparkJobsConfig Jobs configuration defined in {@code sparkjobs.properties} file
   * @param containersService a service able to run Spark jobs in separate container.
   */
  public JobsService(final SparkConfig sparkConfig, final SparkJobsConfig sparkJobsConfig, final ContainersService containersService) {
    this.sparkConfig = sparkConfig;
    this.sparkJobsConfig = sparkJobsConfig;
    this.containersService = containersService;
  }

  public Promise<Result<JobValues>> apply(final JobRequest jobRequest) {
    Objects.requireNonNull(jobRequest.getMode());
    Objects.requireNonNull(jobRequest.getCodeName());
    return containersService
      .getJobContainer(jobRequest.getCodeName(), sparkJobsConfig.getJobs().get(jobRequest.getCodeName()))
      .<JobValues>flatMap(container -> {
        UUID uuid = UUID.randomUUID();
        ImmutableMap<String, String> params = ImmutableMap.<String, String>builder()
          .put("jobId", uuid.toString())
          .putAll(JobParam.to(jobRequest.getParams()))
          .build();

        if (jobRequest.getMode() == JobExecMode.SYNC) {
          return container
            .runJob(params)
            .flatMap(result -> container
              .<List<List<String>>>fetchJobResults(params)
              .map(values -> JobValues.of(uuid, JobExecStatus.FINISHED, JobValues.JobValue.to(values))));
        } else {
          // execute job in background
          Execution.fork().start(forkedExec -> {
            container
              .runJob(params)
              .then(result -> {
                LOGGER.debug("JOB [{}] FINISHED", uuid.toString());
              });
          });
          return Promise.value(JobValues.of(uuid, JobExecStatus.WORKING));
        }
      })
      .map(Result::success);
  }
}
