package ratpack.hadoop.spark.func.topn.model;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import java.util.List;

/**
 * User activity counter. Counts all requests sent by a user.
 */
@Getter
@AllArgsConstructor
@ToString
@EqualsAndHashCode
public class UserActivityCounter {
  private final String username;
  private final Integer counter;

  public static UserActivityCounter of(String... args) {
    if (args.length < 1) {
      return null;
    } else if (args.length < 2) {
      return new UserActivityCounter(args[0], null);
    } else {
      return new UserActivityCounter(args[0], Integer.valueOf(args[1]));
    }
  }

  public static UserActivityCounter of(List<String> args) {
    return of(args.toArray(new String[]{}));
  }
}
