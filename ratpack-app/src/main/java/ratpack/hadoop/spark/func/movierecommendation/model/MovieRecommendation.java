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
package ratpack.hadoop.spark.func.movierecommendation.model;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import java.util.List;

/**
 * Recommended movie.
 */
@Getter
@AllArgsConstructor
@ToString
@EqualsAndHashCode
public class MovieRecommendation {
  private final Integer movieId;
  private final Double rating;
  private final String movieTitle;

  public static MovieRecommendation of(String... args) {
    if (args.length < 1) {
      return null;
    } else if (args.length < 2) {
      return new MovieRecommendation(Integer.parseInt(args[0]), null, null);
    } else if (args.length < 3) {
      return new MovieRecommendation(Integer.parseInt(args[0]), Double.parseDouble(args[1]), null);
    } else {
      return new MovieRecommendation(Integer.parseInt(args[0]), Double.parseDouble(args[1]), args[2]);
    }
  }

  public static MovieRecommendation of(List<String> args) {
    return of(args.toArray(new String[] {}));
  }
}
