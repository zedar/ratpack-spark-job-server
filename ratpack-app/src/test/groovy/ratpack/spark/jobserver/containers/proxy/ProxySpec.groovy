package ratpack.spark.jobserver.containers.proxy

import spock.lang.IgnoreRest
import spock.lang.Specification

import java.nio.file.Path
import java.nio.file.Paths

class ProxySpec extends Specification {
  Path testClassesPath

  def setup() {
    Path currentPath = Paths.get("").toAbsolutePath()
    testClassesPath = currentPath.resolve("build/classes/test").toAbsolutePath()
  }

  def "get class by test classes class loader"() {
    expect:
    testClassesPath.toFile().exists()

    when:
    URLClassLoader targetCL = ProxyObjectThroughInterface.getDirClassLoader(testClassesPath)

    then:
    targetCL
    println targetCL.URLs

    when:
    Class proxyClass = ProxyObjectThroughInterface.loadClass(targetCL, "ratpack.spark.jobserver.containers.proxy.SimpleConfig")

    then:
    proxyClass
  }

  def "get proxy for simple interface"() {
    given:
    URLClassLoader targetCL = ProxyObjectThroughInterface.getDirClassLoader(testClassesPath)

    when:
    SimpleConfigInterface simpleConfig = ProxyObjectThroughInterface.of(SimpleConfigInterface.class, targetCL, "ratpack.spark.jobserver.containers.proxy.SimpleConfig")

    then:
    simpleConfig

    when:
    simpleConfig.setName("Foo")
    simpleConfig.setValue("Bar")

    then:
    simpleConfig.getName() == "Foo"
    simpleConfig.getValue() == "Bar"
    println "SimpleConfig: ${simpleConfig.toString()}"
  }

  def "get proxy for object with declared constructor"() {
    given:
    URLClassLoader targetCL = ProxyObjectThroughInterface.getDirClassLoader(testClassesPath)

    when:
    SimpleConfigAttrInterface proxy = ProxyObjectThroughInterface.of(
      SimpleConfigAttrInterface.class,
      targetCL,
      "ratpack.spark.jobserver.containers.proxy.SimpleConfigAttr",
      ["java.lang.String", "java.lang.String"] as String[],
      ["foo", "bar"] as String[]
    )

    then:
    proxy
    proxy.getName() == "foo"
    proxy.getValue() == "bar"
  }

  def "get proxy for object with declared constructor with object"() {
    given:
    URLClassLoader targetCL = ProxyObjectThroughInterface.getDirClassLoader(testClassesPath)

    when:
    SimpleConfigAttrInterface proxyAttr = ProxyObjectThroughInterface.of(
      SimpleConfigAttrInterface.class,
      targetCL,
      "ratpack.spark.jobserver.containers.proxy.SimpleConfigAttr",
      ["java.lang.String", "java.lang.String"] as String[],
      ["foo", "bar"] as String[]
    )
    SimpleConfigInterface proxy = ProxyObjectThroughInterface.of(
      SimpleConfigInterface.class,
      targetCL,
      "ratpack.spark.jobserver.containers.proxy.SimpleConfig",
      ["ratpack.spark.jobserver.containers.proxy.SimpleConfigAttr"] as String[],
      [proxyAttr.getTarget()] as Object[]
    )

    then:
    proxy
    proxy.getName() == "foo"
    proxy.getValue() == "bar"
  }

  @IgnoreRest
  def "set object attribute for proxied interface"() {
    given:
    URLClassLoader targetCL = ProxyObjectThroughInterface.getDirClassLoader(testClassesPath)

    when:
    SimpleConfigAttrInterface proxyAttr1 = ProxyObjectThroughInterface.of(
      SimpleConfigAttrInterface.class,
      targetCL,
      "ratpack.spark.jobserver.containers.proxy.SimpleConfigAttr",
      ["java.lang.String", "java.lang.String"] as String[],
      ["foo", "bar"] as String[]
    )
    SimpleConfigAttrInterface proxyAttr2 = ProxyObjectThroughInterface.of(
      SimpleConfigAttrInterface.class,
      targetCL,
      "ratpack.spark.jobserver.containers.proxy.SimpleConfigAttr",
      ["java.lang.String", "java.lang.String"] as String[],
      ["baz", "qux"] as String[]
    )
    SimpleConfigInterface proxy = ProxyObjectThroughInterface.of(
      SimpleConfigInterface.class,
      targetCL,
      "ratpack.spark.jobserver.containers.proxy.SimpleConfig",
      ["ratpack.spark.jobserver.containers.proxy.SimpleConfigAttr"] as String[],
      [proxyAttr1.getTarget()] as Object[]
    )

    then:
    proxy
    proxy.getName() == "foo"
    proxy.getValue() == "bar"
    println("1: ${proxy.toString()}")

    when:
    proxy.setAttr(proxyAttr2.getTarget())

    then:
    proxy.getName() == "baz"
    proxy.getValue() == "qux"
    println("2: ${proxy.toString()}")

    when:
    proxy.setName("quux")
    proxy.setValue("corge")

    then:
    proxy.getName() == "quux"
    proxy.getValue() == "corge"
    println("3: ${proxy.toString()}")
  }
}
