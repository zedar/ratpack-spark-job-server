package ratpack.spark.jobserver.containers

import ratpack.groovy.server.GroovyRatpackServerSpec
import ratpack.groovy.test.embed.GroovyEmbeddedApp
import ratpack.guice.Guice
import ratpack.server.RatpackServer
import ratpack.server.ServerConfig
import ratpack.spark.jobserver.SparkConfig
import ratpack.spark.jobserver.SparkJobsConfig
import spock.lang.IgnoreRest
import spock.lang.Specification

class ContainersLifecycleSpec extends Specification {
  SparkConfig sparkConfig
  SparkJobsConfig sparkJobsConfig
  ContainersService containersService

  def setup() {
    sparkConfig = new SparkConfig()
    sparkJobsConfig = new SparkJobsConfig()
    containersService = Mock(ContainersService)
  }

  @IgnoreRest
  def "containers lifecycle established"() {
    given:
    def app = GroovyEmbeddedApp.of {
      registryOf {
        add(new ContainersLifecycle(sparkConfig, sparkJobsConfig, containersService))
      }
    }

    when:
    app.server.start()
    app.server.stop()

    then:
    1 * containersService.onStop()
  }
}
