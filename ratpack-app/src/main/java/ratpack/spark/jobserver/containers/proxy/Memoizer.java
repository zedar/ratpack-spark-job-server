package ratpack.spark.jobserver.containers.proxy;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

/**
 * Memoizer functor for any data provided by a {@link java.util.function.Function}
 */
public interface Memoizer {
  static <I,O> Function<I,O> memoize(Function<I,O> fn) {
    ConcurrentMap<I,O> cache = new ConcurrentHashMap<>();
    return i -> cache.computeIfAbsent(i, fn);
  }
}
