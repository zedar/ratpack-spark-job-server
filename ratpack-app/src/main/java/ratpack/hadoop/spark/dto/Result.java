package ratpack.hadoop.spark.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

/**
 * Result of API call, serializable to {@code JSON}
 */
@AllArgsConstructor
@ToString
@Getter
public class Result<T> {
  private final String errorCode;
  private final String errorMessage;
  private final T value;

  public static <T> Result<T> of(ratpack.exec.Result<T> result) {
    if (result.isSuccess()) {
      return new Result<>("0", null, result.getValue());
    } else {
      return new Result<>(result.getThrowable().getMessage(), result.getThrowable().getCause() != null ? result.getThrowable().getCause().getMessage() : result.getThrowable().getMessage(), null);
    }
  }
}
