package ratpack.spark.jobserver.containers.proxy;

import java.util.function.Function;

/**
 * Category Theory: 3 elements functor
 */
public interface Functor3<T0,T1,T2,F extends Functor3<?,?,?,F>> {
  <R0,R1,R2> F map(Function<T0,R0> fn0, Function<T1,R1> fn1, Function<T2,R2> fn2);
  <R0,R1,R2> F map(Function<T0,Function<T1, Function<T2, Tuple3<R0,R1,R2>>>> fn);
}
