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
import ratpack.spark.jobserver.jobs.model.*;

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
  private final JobsRepository jobsRepository;

  /**
   * The constructor
   * @param sparkConfig Spark configuration defined in {@code application.properties} file
   * @param sparkJobsConfig Jobs configuration defined in {@code sparkjobs.properties} file
   * @param containersService a service able to apply Spark jobs in separate container.
   */
  public JobsService(final SparkConfig sparkConfig, final SparkJobsConfig sparkJobsConfig, final ContainersService containersService, final JobsRepository jobsRepository) {
    this.sparkConfig = sparkConfig;
    this.sparkJobsConfig = sparkJobsConfig;
    this.containersService = containersService;
    this.jobsRepository = jobsRepository;
  }

  /**
   * Execute spark job defined by {@code jobRequest}
   * @param jobRequest data transfer object defining job execution criteria
   * @return the promise for job execution result
   * @throws Exception any
   */
  public Promise<Result<Job>> apply(final JobRequest jobRequest) throws Exception{
    Objects.requireNonNull(jobRequest.getMode());
    Objects.requireNonNull(jobRequest.getCodeName());
    return containersService
      .getJobContainer(jobRequest.getCodeName(), sparkJobsConfig.getJobs().get(jobRequest.getCodeName()))
      .<Job>flatMap(container -> {
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
              .map(values -> Job.of(uuid, JobExecStatus.FINISHED, Job.JobValue.to(values))))
            .flatMap(job -> jobsRepository.saveJob(job));
        } else {
          // execute job in background
          Execution.fork().start(forkedExec -> {
            container
              .runJob(params)
              .flatMap(result -> jobsRepository
                  .findJob(uuid.toString())
                  .onNull(() -> Result.error(new IllegalArgumentException("JOB NOT REGISTERED id: " + uuid.toString())))
                  .map(job -> job.jobStatus(JobExecStatus.FINISHED))
              )
              .then(job -> {
                LOGGER.debug("ASYNC EXECUTION: JOB [{}]", job.toString());
              });
          });
          return Promise
            .value(Job.of(uuid, JobExecStatus.WORKING))
            .flatMap(job -> jobsRepository.saveJob(job));
        }
      })
      .map(Result::success);
  }

  /**
   * Get job by its id
   * @param jobId registed job id
   * @return the promise for result of job execution
   * @throws Exception any
   */
  public Promise<Result<Job>> get(final String jobId) throws Exception {
    Objects.requireNonNull(jobId);
    return jobsRepository
      .findJob(jobId)
      .onNull(() -> Result.error(new IllegalArgumentException("JOB UNDEFINED id: " + jobId)))
      .map(Result::success);
  }
}
