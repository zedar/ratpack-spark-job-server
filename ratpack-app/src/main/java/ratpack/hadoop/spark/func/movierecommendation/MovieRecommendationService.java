package ratpack.hadoop.spark.func.movierecommendation;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ratpack.exec.Promise;
import ratpack.exec.Result;
import ratpack.hadoop.spark.SparkConfig;
import ratpack.hadoop.spark.SparkJobsConfig;
import ratpack.hadoop.spark.containers.ContainersService;
import ratpack.hadoop.spark.func.movierecommendation.dto.Request;
import ratpack.hadoop.spark.func.movierecommendation.model.MovieRecommendation;

import java.util.List;

/**
 * Executes movie recommendation algorithm for the given user. Returns defined number of recommended movies
 */
public class MovieRecommendationService {
  public static final Logger LOGGER = LoggerFactory.getLogger(MovieRecommendationService.class);

  private final SparkConfig config;
  private final SparkJobsConfig sparkJobsConfig;
  private final ContainersService containersService;

  /**
   * The constructor
   * @param config spark configuration
   * @param sparkJobsConfig spark jobs configuration
   * @param containersService a service to manage spark job containers
   */
  public MovieRecommendationService(final SparkConfig config, final SparkJobsConfig sparkJobsConfig, final ContainersService containersService) {
    this.config = config;
    this.sparkJobsConfig = sparkJobsConfig;
    this.containersService = containersService;
  }

  public Promise<Result<List<MovieRecommendation>>> apply(Request request, String inputFS, String outputFS) {
    return containersService
      .getJobContainer("MovieRecommendation", sparkJobsConfig.getMovieRecommendationJarsDir(), "spark.func.movierecommendation.MovieRecommendationApp")
      .flatMap(container -> {
        String inputPath = config.getHDFSURI(Strings.isNullOrEmpty(inputFS) ? "input" : inputFS);
        String outputPath = config.getHDFSURI(Strings.isNullOrEmpty(outputFS) ? "output" : outputFS);
        ImmutableMap<String, String> params = ImmutableMap.of(
          "userId", request.getUserId().toString(),
          "limit", request.getLimit().getValue().toString());
        return container
          .runJob(params, inputPath, outputPath)
          .flatMap(uuid -> container.<List<List<String>>>fetchJobResults(outputPath, uuid));
      })
      .map(results -> {
        List<MovieRecommendation> movieRecommendations = Lists.newArrayList();
        if (results != null) {
          for (List<String> result : results) {
            MovieRecommendation movieRecommendation = MovieRecommendation.of(result);
            LOGGER.debug("RESULT RECORD: {}", movieRecommendation.toString());
            movieRecommendations.add(movieRecommendation);
          }
        }
        return ImmutableList.copyOf(movieRecommendations);
      })
      .map(Result::success);
  }
}
