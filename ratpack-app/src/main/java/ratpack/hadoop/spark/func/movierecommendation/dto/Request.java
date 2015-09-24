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
package ratpack.hadoop.spark.func.movierecommendation.dto;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.ToString;
import ratpack.hadoop.spark.model.Limit;

/**
 * Parameters for executing Movie Recommendation Algorithm,
 * This is immutable data transfer object.
 */
@Getter
@ToString
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Request {
  private final Integer userId;
  private final Limit limit;

  private Request(final Integer userId, final Limit limit) {
    this.userId = userId;
    this.limit = limit;
  }

  /**
   * Creates the data transform object as a request to movie recommendation service.
   * <p>
   * It is used as factory method {@link JsonCreator} during the JSON deserialization phase.
   *
   * @param userId a user id to find movie recommendations
   * @param limit a number of movies to recommend
   * @return the Request for movie recommendation service
   */
  @JsonCreator
  public static Request of(@JsonProperty("userId") int userId, @JsonProperty("limit") int limit) {
    return new Request(Integer.valueOf(userId), Limit.of(limit));
  }
}
