package ratpack.spark.jobserver.jobs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ratpack.func.Action;
import ratpack.handling.Chain;
import ratpack.spark.jobserver.SparkConfig;
import ratpack.spark.jobserver.SparkJobsConfig;
import ratpack.spark.jobserver.dto.Result;
import ratpack.spark.jobserver.jobs.dto.JobRequest;

import javax.inject.Inject;
import java.rmi.UnexpectedException;

import static ratpack.jackson.Jackson.fromJson;
import static ratpack.jackson.Jackson.json;

/**
 * {@code /jobs} endpoints chain.
 */
public class JobsEndpoints implements Action<Chain> {
  private static final Logger LOGGER = LoggerFactory.getLogger(JobsEndpoints.class);

  private final JobsService jobsService;

  /**
   * Constructor and its injected dependencies
   *
   * @param jobsService a reference to jobs service
   */
  @Inject
  public JobsEndpoints(final JobsService jobsService) {
    this.jobsService = jobsService;
  }

  @Override
  public void execute(Chain chain) throws Exception {
    chain
      .path("jobs", ctx -> {
        ctx.byMethod(spec -> spec
          .post(() -> ctx
            .parse(fromJson(JobRequest.class))
            .onError(ex -> {
              ex.printStackTrace();
              ctx.render(json(Result.of(ratpack.exec.Result.error(ex))));
            })
            .onNull(() -> ctx.render(json(Result.of(ratpack.exec.Result.error(new UnexpectedException("INPUT PARSING ERROR"))))))
            .flatMap(request -> jobsService.apply(request))
            .map(Result::of)
            .map(r -> json(r))
            .then(ctx::render))
          .get(() -> jobsService
            .getAll()
            .map(Result::of)
            .map(r -> json(r))
            .then(ctx::render)));
      })
      .path("jobs/:job_id", ctx -> {
        String jobId = ctx.getPathTokens().get("job_id");
        LOGGER.debug("GET JOB id: {}", jobId);
        ctx.byMethod(spec -> spec
          .get(() -> jobsService
            .get(jobId)
            .onError(ex -> {
              ex.printStackTrace();
              ctx.render(json(Result.of(ratpack.exec.Result.error(ex))));
            })
            .map(Result::of)
            .map(r -> {
              LOGGER.debug("RESULT: {}", r.toString());
              return json(r);
            })
            .then(ctx::render)));
      });
  }
}
