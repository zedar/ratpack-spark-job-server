package ratpack.spark.jobserver.containers.proxy;

import lombok.*;
import lombok.experimental.Accessors;

import java.util.function.Function;

/**
 * Tuple with 1 value
 */
@Getter
@Accessors(chain = true, fluent = true)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@ToString
@EqualsAndHashCode
public final class Tuple1<T0> implements Tuple<Tuple1<T0>>, Functor1<T0, Tuple1<?>> {
  private final T0 _0;

  @Override
  public int size() {
    return 1;
  }

  @Override
  public Object apply(int value) {
    if (value == 0) {
      return _0();
    } else {
      throw new IndexOutOfBoundsException();
    }
  }

  @Override
  public <R0> Tuple1<R0> map(Function<T0, R0> fn) {
    return of(fn.apply(_0()));
  }

  public static <T0> Tuple1<T0> of(T0 _0) {
    return new Tuple1<>(_0);
  }
}
