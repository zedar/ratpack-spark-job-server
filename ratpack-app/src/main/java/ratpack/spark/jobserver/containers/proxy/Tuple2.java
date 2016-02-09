package ratpack.spark.jobserver.containers.proxy;

import lombok.*;
import lombok.experimental.Accessors;

import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Tuple with 2 values
 */
@Getter
@Accessors(chain = true, fluent = true)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@ToString
@EqualsAndHashCode
public final class Tuple2<T0, T1> implements Tuple<Tuple2<T0, T1>>, Functor2<T0,T1, Tuple2<?,?>> {
  private final T0 _0;
  private final T1 _1;

  @Override
  public int size() {
    return 2;
  }

  @Override
  public Object apply(int value) {
    switch (value) {
      case 0: return _0();
      case 1: return _1();
      default: throw new IndexOutOfBoundsException();
    }
  }

  @Override
  public <R0, R1> Tuple2<R0, R1> map(Function<T0, R0> fn0, Function<T1, R1> fn1) {
    return of(fn0.apply(_0()), fn1.apply(_1()));
  }

  @Override
  public <R0, R1> Tuple2<R0, R1> map(BiFunction<T0, T1, Tuple2<R0, R1>> fn) {
    return fn.apply(_0(), _1());
  }

  public static <T0, T1> Tuple2<T0, T1> of(T0 _0, T1 _1) {
    return new Tuple2<>(_0, _1);
  }
}
