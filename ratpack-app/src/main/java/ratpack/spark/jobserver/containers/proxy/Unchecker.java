package ratpack.spark.jobserver.containers.proxy;

import java.util.function.Function;

/**
 * Unchekc functor. Calls a function and maps all checked exception to uncheked (RuntimeExceptions)
 */
public interface Unchecker {
  static <I,O> Function<I,O> uncheck(ratpack.func.Function<I,O> fn) {
    return i -> {
      try {
        return fn.apply(i);
      } catch (Throwable th) {
        if (th instanceof RuntimeException) {
          throw (RuntimeException)th;
        } else {
          throw new RuntimeException(th);
        }
      }
    };
  }
}
