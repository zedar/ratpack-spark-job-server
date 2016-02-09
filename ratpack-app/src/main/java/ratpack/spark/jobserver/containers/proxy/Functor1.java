package ratpack.spark.jobserver.containers.proxy;

import java.util.function.Function;

/**
 * Category theory: functor maps between categories (object (elements of categories), arrows (morphisms) and preserves compasability and identity)
 */
public interface Functor1<T, F extends Functor1<?,?>> {
  <R> F map(Function<T,R> fn);
}
