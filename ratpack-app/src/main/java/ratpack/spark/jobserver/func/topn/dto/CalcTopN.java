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
package ratpack.spark.jobserver.func.topn.dto;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.ToString;
import ratpack.spark.jobserver.model.TimeInterval;
import ratpack.spark.jobserver.model.Limit;

/**
 * Parameters for executing top-n map reduce calculation.
 * This is simple and immutable data transfer object.
 */
@Getter
@ToString
@JsonInclude(JsonInclude.Include.NON_NULL)
public class CalcTopN {
  private final Limit limit;
  private final TimeInterval timeInterval;

  private CalcTopN(final Limit limit, final TimeInterval timeInterval) {
    this.limit = limit;
    this.timeInterval = timeInterval;
  }

  /**
   * Creates the data transform object for top-n calculation with limit but without time interval.
   * @param limit a limit for top-n. It is {@code n} in top-n
   * @return the DTO for top-n calculation
   */
  public static CalcTopN of(int limit) {
    return new CalcTopN(Limit.of(limit), null);
  }

  /**
   * Creates the data transform object for top-n calculation with limit and optional time interval.
   * <p>
   * It is used as {@code JsonCreator} factory method used for deserialization from {@code JSON}.
   * @param limit a limit for top-n. It is {@code n} in top-n
   * @param timeInterval a time interval with date from and date to
   * @return the DTO for top-n calculation
   */
  @JsonCreator
  public static CalcTopN of(@JsonProperty("limit")int limit, @JsonProperty("timeInterval") TimeInterval timeInterval) {
    if (timeInterval == null) {
      return new CalcTopN(Limit.of(limit), null);
    } else {
      return new CalcTopN(Limit.of(limit), timeInterval);
    }
  }
}
