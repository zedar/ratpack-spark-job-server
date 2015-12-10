package ratpack.spark.jobserver.jobs

import groovy.json.JsonSlurper
import ratpack.http.MediaType
import ratpack.spark.jobserver.Main
import ratpack.test.ApplicationUnderTest
import ratpack.test.MainClassApplicationUnderTest
import ratpack.test.http.TestHttpClient
import spock.lang.Shared
import spock.lang.Specification

class JobsEndpointsApiDefSpec extends Specification {
  @Shared
  ApplicationUnderTest aut = MainClassApplicationUnderTest.of(Main)

  @Delegate
  TestHttpClient testHttpClient = aut.httpClient

  JsonSlurper json

  def setup() {
    json = new JsonSlurper()
  }

  def "api-def is accessible and defines endpoints"() {
    when:
    getText("v1/api-def")

    then:
    response.body.contentType?.toString() == MediaType.APPLICATION_JSON.toString()
    def root = json.parseText(getText("v1/api-def"))
    root instanceof Map
    root.paths
    root.paths."/v1/spark/jobs/{job_id}"  instanceof Map
    root.paths."/v1/spark/jobs" instanceof Map
  }
}