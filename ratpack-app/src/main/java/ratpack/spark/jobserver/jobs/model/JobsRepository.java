package ratpack.spark.jobserver.jobs.model;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import ratpack.exec.Promise;
import ratpack.exec.Result;

import java.util.Objects;

/**
 * Repository for Spark Jobs executed by the Spark Job Server.
 */
public class JobsRepository {
  private Cache<String, Job> cache = CacheBuilder.newBuilder().initialCapacity(10).build();

  public JobsRepository() {

  }

  public Promise<Job> saveJob(final Job job) throws Exception {
    Objects.requireNonNull(job);
    cache.put(job.getJobId(), job);
    return Promise.value(cache.getIfPresent(job.getJobId()));
  }

  public Promise<Job> findJob(final String jobId) throws Exception {
    return Promise.value(cache.getIfPresent(jobId));
  }
}
