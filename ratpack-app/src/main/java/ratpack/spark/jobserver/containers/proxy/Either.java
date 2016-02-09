package ratpack.spark.jobserver.containers.proxy;

import lombok.*;
import lombok.experimental.Accessors;

import java.util.Optional;
import java.util.function.Function;

/**
 * Category Theory: either of two elements is present
 */
@Getter
@Accessors(chain = true, fluent = true)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@ToString
@EqualsAndHashCode
public final class Either<L,R> {
  private final Optional<L> left;
  private final Optional<R> right;

  public static <L,R> Either<L,R> left(L left) {
    return new Either<>(Optional.of(left), Optional.empty());
  }

  public static <L,R> Either<L,R> right(R right) {
    return new Either<>(Optional.<L>empty(), Optional.of(right));
  }

  public <T> T map(Function<L,T> lfn, Function<R,T> rfn) {
    return left.map(lfn).orElseGet(() -> right.map(rfn).get());
  }

  public <T> Either<T,R> mapL(Function<L,T> fn) {
    return new Either<>(left.map(fn), right);
  }

  public <T> Either<L,T> mapR(Function<R,T> fn) {
    return new Either<>(left, right.map(fn));
  }
}
