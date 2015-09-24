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
package ratpack.hadoop.spark.func.topn;

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
import ratpack.hadoop.spark.func.topn.dto.CalcTopN;
import ratpack.hadoop.spark.func.topn.model.UserActivityCounter;
import java.util.ArrayList;
import java.util.List;

/**
 * Executes Top-N algorithm with Apache Spark
 */
public class TopNService {
  private static final Logger LOGGER = LoggerFactory.getLogger(TopNService.class);

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
   * @param request a request with a limit for the number of the most active users and/or a time interval for looking for the most active users
   * @param inputFS a hadoop file system where user activity logs are stored
   * @param outputFS a hadoop file system where calculation results are stored
   * @return the promise for the result of top-n map-reduce calculation
   */
  public Promise<Result<List<UserActivityCounter>>> apply(String jobName, CalcTopN request, String inputFS, String outputFS) {
    return containersService
      .getJobContainer(Strings.isNullOrEmpty(jobName) ? "TopN" : jobName, sparkJobsConfig.getTopNClassName())
      .flatMap(container -> {
        String inputPath = config.getHDFSURI(Strings.isNullOrEmpty(inputFS) ? "input" : inputFS);
        String outputPath = config.getHDFSURI(Strings.isNullOrEmpty(outputFS) ? "output" : outputFS);
        ImmutableMap<String, String> params = ImmutableMap.of(
          "limit", request.getLimit().getValue().toString(),
          "dateFrom", request.getTimeInterval() != null && request.getTimeInterval().getDateFrom() != null ? request.getTimeInterval().getDateFrom().toString() : "",
          "dateTo",   request.getTimeInterval() != null && request.getTimeInterval().getDateTo() != null ? request.getTimeInterval().getDateTo().toString() : "");
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
