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
package spark.func.movierecommendation;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Calculation recommendation for the given user based on the set of recommendations.
 * Uses Machine Learning Library from Apache Spark
 */
public class MovieRecommendationApp {
  private static final Logger LOGGER = LoggerFactory.getLogger(MovieRecommendationApp.class);

  private static final String RESULT_REGEX = "\\(Rating\\(([\\d]+),([\\d]+),([-+]?[0-9]*\\.?[0-9]+)\\),(.+)\\)";
  private static final Pattern RESULT_PATTERN = Pattern.compile(RESULT_REGEX);

  public static void runJob(Configuration configuration, JavaSparkContext sparkContext, Map<String, String> params, String inputPath, String outputPath) throws Exception {
    Objects.requireNonNull(params);

    LOGGER.info("JOB STARTED");
    LOGGER.debug("JOB PREPARE OUTPUT");
    if (configuration != null) {
      FileSystem fileSystem = FileSystem.get(configuration);
      fileSystem.delete(new Path(outputPath), false);
    }

    // read user-item ratings and create Rating-s
    JavaRDD<Rating> ratings = sparkContext.textFile(inputPath + "/ratings.csv")
      .flatMap(s -> {
        // skip header
        if (s.contains("userId")) {
          return Collections.emptyList();
        } else {
          String[] strings = s.split(",");
          return Arrays.asList(new Rating(Integer.parseInt(strings[0]), Integer.parseInt(strings[1]), Double.parseDouble(strings[2])));
        }
      });

    // read item description
    JavaPairRDD<Integer, String> items = sparkContext.textFile(inputPath + "/movies.csv")
      .<Integer, String>flatMapToPair(s -> {
        // skip header
        if (s.contains("movieId")) {
          return Collections.emptyList();
        } else {
          String[] strings = s.split(",");
          return Arrays.asList(new Tuple2<Integer, String>(Integer.parseInt(strings[0]), strings[1]));
        }
      });

    // build the recommendation model using Alternate Least Square Method
    int rank = 10; // number of latent factors
    int numOfIters = 5; // number of iterations to train the model

    MatrixFactorizationModel model = ALS.trainImplicit(JavaRDD.toRDD(ratings), rank, numOfIters);

    // find movies not rated by the given user
    Integer limit = Integer.parseInt(params.getOrDefault("limit", "10"));

    Integer uId = Integer.parseInt(params.getOrDefault("userId", "0"));
    Broadcast<Integer> userId = sparkContext.broadcast(uId);

    // userid-movieid that are not rated by the given user
    JavaRDD<Tuple2<Integer, Integer>> itemsNotRatedByUser = ratings
      .map(r -> new Tuple2<Integer, Integer>(r.user(), r.product()))
      .filter(t -> !t._1().equals(userId.getValue()))
      .map(t2 -> new Tuple2<Integer, Integer>(userId.getValue(), t2._2()));

    // predict the ratings of the not rated items
    JavaRDD<Rating> recommendations = model.predict(JavaPairRDD.fromJavaRDD(itemsNotRatedByUser)).distinct();

    JavaRDD<Tuple2<Rating, String>> recommendedItems = recommendations
      .mapToPair(r -> new Tuple2<Integer, Rating>(r.product(), r))
      .join(items)
      .values();

    sparkContext
      .parallelize(recommendedItems.takeOrdered(limit, new RatingComparator()))
      .saveAsTextFile(outputPath);

    LOGGER.info("JOB FINISHED");
  }

  public static List<List<String>> fetchJobResults(Configuration configuration, String outputPath) throws Exception {
    if (configuration == null) {
      return Lists.newArrayList();
    }
    FileSystem fileSystem = FileSystem.get(configuration);
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
        Matcher matcher = RESULT_PATTERN.matcher(line);
        if (matcher.find()) {
          // group 1 - userid, group 2 - bookid, group 3 - rating, group 4 - title
          LOGGER.debug("SPARK RESULT: {} {} {}", matcher.group(2), matcher.group(3), matcher.group(4));
          results.add(Lists.newArrayList(matcher.group(2), matcher.group(3), matcher.group(4)));
        }
        line = br.readLine();
      }
      is.close();
    }

    fileSystem.delete(new Path(outputPath), true);

    return results;
  }

  private static class RatingComparator implements Comparator<Tuple2<Rating, String>>, Serializable {
    @Override
    public int compare(Tuple2<Rating, String> o1, Tuple2<Rating, String> o2) {
      return Double.compare(o2._1().rating(), o1._1().rating());
    }
  }
}
