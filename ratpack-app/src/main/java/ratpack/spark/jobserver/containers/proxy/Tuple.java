package ratpack.spark.jobserver.containers.proxy;

import java.io.Serializable;
import java.util.Objects;
import java.util.function.IntFunction;

/**
 * Basic class for tuples
 */
public interface Tuple<T extends Tuple> extends IntFunction, Serializable, Cloneable, Comparable<T>  {
  int size();

  @Override
  default int compareTo(T o) {
    Objects.requireNonNull(o);
    if (!getClass().equals(o.getClass())) {
      throw new ClassCastException(o.getClass() + " must equals to " + getClass());
    }
    for (int i = 0; i < size(); i++) {
      Comparable<Object> l = (Comparable<Object>)apply(i);
      Object r = o.apply(i);
      int c = l.compareTo(r);
      if (c != 0) {
        return c;
      }
    }
    return 0;
  }

  public static <T0> Tuple1<T0> of(T0 _0) {
    return Tuple1.of(_0);
  }

  public static <T0, T1> Tuple2<T0, T1> of(T0 _0, T1 _1) {
    return Tuple2.of(_0, _1);
  }

  public static <T0, T1, T2> Tuple3<T0, T1, T2> of(T0 _0, T1 _1, T2 _2) {
    return Tuple3.of(_0, _1, _2);
  }
}
