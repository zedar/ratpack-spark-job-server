package ratpack.spark.jobserver.jobs.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Parameter of a job execution.
 */
@Getter
@ToString
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class JobParam {
  private final String name;
  private final String value;

  /**
   * Convert list of job params to map of string to string
   * @param params list of job parameters
   * @return immutable map of key/value pairs
   */
  public static Map<String, String> to(List<JobParam> params) {
    if (params == null) {
      return ImmutableMap.of();
    }
    Map<String, String> outParams = params
      .stream()
      .collect(Collectors.toMap(JobParam::getName, JobParam::getValue));
    return ImmutableMap.copyOf(outParams);
  }

  /**
   * Creates a job param out of the {@code JSON} values.
   * @param name a param name
   * @param value a param value
   * @return the created job param
   */
  @JsonCreator
  public static JobParam of(@JsonProperty("name") String name, @JsonProperty("value") String value) {
    return new JobParam(name, value);
  }
}
