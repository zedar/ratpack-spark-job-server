package ratpack.hadoop.spark.func.topn;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ratpack.exec.Blocking;
import ratpack.exec.Promise;
import ratpack.exec.Result;
import ratpack.hadoop.spark.SparkConfig;
import ratpack.hadoop.spark.SparkJobsConfig;
import ratpack.hadoop.spark.containers.ContainersService;
import ratpack.hadoop.spark.model.TimeInterval;
import ratpack.hadoop.spark.func.topn.model.UserActivityCounter;
import ratpack.hadoop.spark.model.Limit;
import ratpack.hadoop.spark.util.ClassUtil;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Executes Top-N algorithm with Apache Spark
 */
public class TopNService {
  private final Logger LOGGER = LoggerFactory.getLogger(TopNService.class);

  private final SparkConfig config;
  private final SparkJobsConfig sparkJobsConfig;
  private final ContainersService containersService;

  public TopNService(final SparkConfig config, final SparkJobsConfig sparkJobsConfig, final ContainersService containersService) {
    this.config = config;
    this.sparkJobsConfig = sparkJobsConfig;
    this.containersService = containersService;
  }

  /**
   * Executes job for calculating top-n users by their activity count.
   * <p>
   * If {@code timeInterval} is provided calculation is executed between {@code dateFrom} and {@code dateTo}
   * @param limit a limit for the number of the most active users
   * @param timeInterval a time interval for looking for the most active users
   * @param inputFS a hadoop file system where user activity logs are stored
   * @param outputFS a hadoop file system where calculation results are stored
   * @return the promise for the result of top-n map-reduce calculation
   */
  public Promise<Result<List<UserActivityCounter>>> apply2(String jobName, Limit limit, TimeInterval timeInterval, String inputFS, String outputFS) {
    return containersService
      .getJobContainer(Strings.isNullOrEmpty(jobName) ? "TopN" : jobName, sparkJobsConfig.getTopNJarsDir(), "spark.func.topn.TopNApp")
      .flatMap(container -> {
        String inputPath = config.getHDFSURI(Strings.isNullOrEmpty(inputFS) ? "input" : inputFS);
        String outputPath = config.getHDFSURI(Strings.isNullOrEmpty(outputFS) ? "output" : outputFS);
        ImmutableMap<String, String> params = ImmutableMap.of(
          "limit", limit.getValue().toString(),
          "dateFrom", (timeInterval != null && timeInterval.getDateFrom() != null ? timeInterval.getDateFrom().toString() : ""),
          "dateTo", (timeInterval != null && timeInterval.getDateTo() != null ? timeInterval.getDateTo().toString() : ""));
        return container
          .runJob(params, inputPath, outputPath)
          .flatMap(uuid -> container.<List<List<String>>>fetchJobResults(outputPath, uuid));
      })
      .map(results -> {
        ArrayList<UserActivityCounter> userActivityCounters = Lists.newArrayList();
        if (results != null) {
          for (List<String> result : results) {
            UserActivityCounter userActivityCounter = UserActivityCounter.of(result);
            LOGGER.debug("RESULT ENTRY: {}", userActivityCounter.toString());
            userActivityCounters.add(userActivityCounter);
          }
        }
        return ImmutableList.copyOf(userActivityCounters);
      })
      .map(Result::success);
  }
}
