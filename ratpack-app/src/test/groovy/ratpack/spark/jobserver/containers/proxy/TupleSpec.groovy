package ratpack.spark.jobserver.containers.proxy

import spock.lang.IgnoreRest
import spock.lang.Specification

import java.util.stream.Stream

class TupleSpec extends Specification {
  def "tuple of 3 create and mapped"() {
    expect:
    SimpleConfig simpleConfig = new SimpleConfig()
    Object[] args = Arrays.asList("name1").toArray()
    Tuple3<Class, String, Object[]> classMethod = Tuple3.of(SimpleConfig.class, "setName", args)

    classMethod._0() == simpleConfig.class
    classMethod._1() == "setName"
    classMethod._2() == args

    Tuple3<Class, String, Class[]> classMethod2 = classMethod.map{c -> c} { m -> m } {attrs -> Stream.of(args).map {o -> o.class }.toArray { s -> new Class[s] } }

    classMethod2._0() instanceof Class
    classMethod2._1() instanceof String
    classMethod2._2() instanceof Class[]
  }

  def "tuple of 3 with nullable argument"() {
    expect:
    SimpleConfig simpleConfig = new SimpleConfig()
    Tuple3<Class, String, Object[]> classMethod = Tuple3.of(SimpleConfig.class, "getName", null)

    classMethod._0() == simpleConfig.class
    classMethod._1() == "getName"
    !classMethod._2()

    Tuple3<Class, String, Class[]> classMethod2 = classMethod.map
      {c -> c}
      { m -> m }
      {args -> Optional.ofNullable(args).map { Stream.of(it).map {o -> o.class }.toArray { s -> new Class[s] } }.orElse(null) }

    classMethod2._0() instanceof Class
    classMethod2._1() instanceof String
    !classMethod2._2()
  }
}