package ratpack.spark.jobserver.containers.proxy;

import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Category Theory: Two elements functor
 */
public interface Functor2<T0,T1,F extends Functor2<?,?,F>> {
  <R0,R1> F map(Function<T0,R0> fn0, Function<T1,R1> fn1);
  <R0,R1> F map(BiFunction<T0,T1, Tuple2<R0,R1>> fn);
}
