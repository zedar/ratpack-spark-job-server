package ratpack.hadoop.spark.topn.dto;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.ToString;
import ratpack.hadoop.spark.topn.model.Limit;
import ratpack.hadoop.spark.topn.model.TimeInterval;

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
