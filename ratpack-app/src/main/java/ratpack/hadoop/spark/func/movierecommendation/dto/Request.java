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
