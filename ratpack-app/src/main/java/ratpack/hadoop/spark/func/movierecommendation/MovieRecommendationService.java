/*
 * Copyright 2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
import java.util.UUID;

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
      .getJobContainer("MovieRecommendation", sparkJobsConfig.getMovieRecommendationClassName())
      .flatMap(container -> {
        String uuid = UUID.randomUUID().toString();
        String inputDir = config.getHDFSURI(Strings.isNullOrEmpty(inputFS) ? "input" : inputFS);
        String outputDir = config.getHDFSURI((Strings.isNullOrEmpty(outputFS) ? "output" : outputFS) + "-" + uuid);
        ImmutableMap<String, String> params = ImmutableMap.of(
          "userId", request.getUserId().toString(),
          "limit", request.getLimit().getValue().toString(),
          "inputDir", inputDir,
          "outputDir", outputDir);
        return container
          .runJob(params)
          .flatMap(result -> container.<List<List<String>>>fetchJobResults(params));
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
