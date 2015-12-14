package ratpack.spark.jobserver.jobs

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import ratpack.jackson.JsonRender
import ratpack.spark.jobserver.SparkConfig
import ratpack.spark.jobserver.SparkJobsConfig
import ratpack.spark.jobserver.containers.ContainersService
import ratpack.spark.jobserver.dto.Result
import ratpack.spark.jobserver.jobs.dto.JobRequest
import ratpack.spark.jobserver.jobs.model.JobsRepository
import ratpack.test.exec.ExecHarness
import ratpack.test.handling.HandlingResult
import ratpack.test.handling.RequestFixture
import spock.lang.AutoCleanup
import spock.lang.Specification

class JobsEndpointsSpec extends Specification {
  RequestFixture requestFixture = RequestFixture.requestFixture()

  SparkConfig sparkConfig
  SparkJobsConfig sparkJobsConfig
  ContainersService containersService
  JobsService jobsService
  JobsRepository jobsRepository
  JobsEndpoints jobsEndpoints

  JsonSlurper json

  def setup() {
    sparkConfig = new SparkConfig()
    sparkJobsConfig = new SparkJobsConfig()
    jobsRepository = new JobsRepository()
    containersService = Mock(ContainersService, constructorArgs: [sparkConfig, sparkJobsConfig])
    jobsService = Mock(JobsService, constructorArgs: [sparkConfig, sparkJobsConfig, containersService, jobsRepository])
    jobsEndpoints = new JobsEndpoints(jobsService)
    json = new JsonSlurper()

    requestFixture.registry { spec -> spec
      .add(SparkConfig, sparkConfig).add(SparkJobsConfig, sparkJobsConfig)
    }
  }

  def "required parameters for /jobs endpoint has to be provided"() {
    given:
    String req = JsonOutput.toJson(
      [notKnown: "not known"]
    )

    when:
    HandlingResult result = requestFixture.uri("jobs").method("POST").body(req, "application/json").handleChain(jobsEndpoints)

    then:
    result.status.code == 200
    JsonRender jr = result.rendered(JsonRender)
    jr
    jr.object
    jr.object instanceof Result
    Result r = (Result)jr.object
    r
    r.errorCode == "JsonMappingException"
    r.errorMessage == "mode parameter is required"

    when:
    req = JsonOutput.toJson([mode: "SYNC"])
    result = requestFixture.uri("jobs").method("POST").body(req, "application/json").handleChain(jobsEndpoints)

    then:
    result.status.code == 200
    JsonRender jr2 = result.rendered(JsonRender)
    jr2
    jr2.object
    jr2.object instanceof Result
    Result r2 = (Result)jr2.object
    r2
    r2.errorCode == "JsonMappingException"
    r2.errorMessage == "codeName parameter is required"
  }

  def "required parameters for /jobs:job_id endpoint has to be provided"() {

  }
}