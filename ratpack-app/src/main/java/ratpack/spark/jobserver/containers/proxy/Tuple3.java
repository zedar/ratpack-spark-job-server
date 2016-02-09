package ratpack.spark.jobserver.containers.proxy;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.Accessors;

import java.util.function.Function;

/**
 * Tuple with 3 values.
 */
@Getter
@Accessors(chain = true, fluent = true)
@AllArgsConstructor
@ToString
@EqualsAndHashCode
public final class Tuple3<T0, T1, T2> implements Tuple<Tuple3<T0, T1, T2>>, Functor3<T0,T1,T2,Tuple3<?,?,?>> {
  private final T0 _0;
  private final T1 _1;
  private final T2 _2;


  @Override
  public int size() {
    return 3;
  }

  @Override
  public Object apply(int value) {
    switch (value) {
      case 0: return _0();
      case 1: return _1();
      case 2: return _2();
      default:throw new IllegalArgumentException();
    }
  }

  @Override
  public <R0, R1, R2> Tuple3<R0, R1, R2> map(Function<T0, R0> fn0, Function<T1, R1> fn1, Function<T2, R2> fn2) {
    return of(fn0.apply(_0()), fn1.apply(_1()), fn2.apply(_2()));
  }

  @Override
  public <R0, R1, R2> Tuple3<R0, R1, R2> map(Function<T0, Function<T1, Function<T2, Tuple3<R0, R1, R2>>>> fn) {
    return fn.apply(_0()).apply(_1()).apply(_2());
  }

  public static <T0, T1, T2> Tuple3<T0, T1, T2> of(T0 _0, T1 _1, T2 _2) {
    return new Tuple3<>(_0, _1, _2);
  }
}
