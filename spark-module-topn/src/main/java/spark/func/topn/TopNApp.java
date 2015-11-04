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
package spark.func.topn;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import spark.jobserver.JobAPI;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Calculate the most active {@code N} users out of the access logs.
 */
public class TopNApp implements JobAPI {
  private static final Logger LOGGER = LoggerFactory.getLogger(TopNApp.class);

  private static final String LIMIT_ARG = "limit";
  private static final String DATE_FROM_ARG = "dateFrom";
  private static final String DATE_TO_ARG = "dateTo";

  private static final String USER_REGEX = "(username=')([A-Za-z0-9]{8}+)(')";
  private static final Pattern USER_PATTERN = Pattern.compile(USER_REGEX);
  private static final String TIME_AND_USER_REGEX = "^(\\[)([0-9]{2}\\/[A-Za-z]{3}\\/[0-9]{4})(.)+(username=')([A-Za-z0-9]{8}+)(')";
  private static final Pattern TIME_AND_USER_PATTERN = Pattern.compile(TIME_AND_USER_REGEX);

  @Override
  public void beforeJob(Configuration hadoopConfig, JavaSparkContext sparkContext, Map<String, String> params) throws Exception {
    String outputPath = getJobOutputPath(hadoopConfig, params);

    if (hadoopConfig != null && !Strings.isNullOrEmpty(outputPath)) {
      FileSystem fileSystem = FileSystem.get(hadoopConfig);
      fileSystem.delete(new Path(outputPath), false);
    }
  }

  @Override
  public void runJob(Configuration hadoopConfig, JavaSparkContext sparkContext, Map<String, String> params) throws Exception {
    LOGGER.info("JOB STARTED");
    String inputPath = getJobInputPath(hadoopConfig, params);
    String outputPath = getJobOutputPath(hadoopConfig, params);
    Integer limit = Integer.valueOf(params.getOrDefault(LIMIT_ARG, "10"));
    // broadcast the compiled pattern
    //Broadcast<Pattern> timeAndUserPattern = sparkContext.broadcast(TIME_AND_USER_PATTERN);

    // broadcast the date from and to across the cluster
    //Broadcast<LocalDate> dateFrom = null, dateTo = null;
    LocalDate df = null;
    LocalDate dt = null;
    if (!Strings.isNullOrEmpty(params.get(DATE_FROM_ARG))) {
      df = LocalDate.parse(params.get(DATE_FROM_ARG));
      //dateFrom = sparkContext.broadcast(df);
    }
    if (!Strings.isNullOrEmpty(params.get(DATE_TO_ARG))) {
      dt = LocalDate.parse(params.get(DATE_TO_ARG));
      //dateTo = sparkContext.broadcast(dt);
    }
    LocalDate dateFromFinal = df;
    LocalDate dateToFinal = dt;
    JavaPairRDD<String, Integer> pairRDD = sparkContext.textFile(inputPath)
      .<String, Integer>flatMapToPair(s -> {
        Matcher matcher = TIME_AND_USER_PATTERN.matcher(s);
        if (matcher.find()) {
          if (dateFromFinal != null || dateToFinal != null) {
            String d = matcher.group(2);
            LocalDate date = LocalDate.parse(d, DateTimeFormatter.ofPattern("dd/MMM/yyyy").withLocale(Locale.ENGLISH));
            if (dateFromFinal != null && date.isBefore(dateFromFinal)) {
              return Collections.emptyList();
            }
            if (dateToFinal != null && date.isAfter(dateToFinal)) {
              return Collections.emptyList();
            }
          }
          return Arrays.asList(new Tuple2<String, Integer>(matcher.group(5), 1));
        } else {
          return Collections.emptyList();
        }
      })
      .reduceByKey(Integer::sum)
      .cache();

    LOGGER.debug("TOTAL COUNT FOR TopN: {}", pairRDD.count());

    List<Tuple2<String, Integer>> result = pairRDD
      .takeOrdered(limit, new ValueComparator());

    sparkContext.parallelizePairs(result).saveAsTextFile(outputPath);

    LOGGER.info("JOB FINISHED");
  }

  @Override
  public List<List<String>> fetchResults(Configuration hadoopConfig, JavaSparkContext sparkContext, Map<String, String> params) throws Exception {
    if (hadoopConfig == null) {
      return Lists.newArrayList();
    }
    String outputPath = getJobOutputPath(hadoopConfig, params);
    FileSystem fileSystem = FileSystem.get(hadoopConfig);
    FileStatus[] status = fileSystem.listStatus(new Path(outputPath), new PathFilter() {
      @Override
      public boolean accept(Path path) {
        return path.getName().startsWith("part");
      }
    });

    List<List<String>> results = Lists.newArrayList();

    for (int i = 0; i < status.length; i++) {
      FSDataInputStream is = fileSystem.open(status[i].getPath());
      BufferedReader br = new BufferedReader(new InputStreamReader(is));
      String line = br.readLine();
      while(line != null) {
        String[] parts = line.replace("(", "").replace(")", "").split(",");
        if (parts.length >= 2) {
          LOGGER.debug("SPARK RESULT: {} {}", parts[0], parts[1]);
          results.add(Lists.newArrayList(parts[0], parts[1]));
        }
        line = br.readLine();
      }
      is.close();
    }

    return results;
  }

  @Override
  public void afterJob(Configuration hadoopConfig, JavaSparkContext sparkContext, Map<String, String> params) throws Exception {
    if (hadoopConfig == null) {
      return;
    }
    String outputPath = getJobOutputPath(hadoopConfig, params);
    FileSystem fileSystem = FileSystem.get(hadoopConfig);
    fileSystem.delete(new Path(outputPath), true);
  }
  /**
   * ValueComparator used for soring by value.
   * Has to be serializable in order to use it in {@link JavaSparkContext#parallelizePairs(List)}
   */
  private static class ValueComparator implements Comparator<Tuple2<String, Integer>>, Serializable {
    public ValueComparator() {
    }

    @Override
    public int compare(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) {
      return t2._2().compareTo(t1._2());
    }
  }
}
