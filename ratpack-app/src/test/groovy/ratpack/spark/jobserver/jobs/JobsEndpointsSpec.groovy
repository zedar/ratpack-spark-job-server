package ratpack.spark.jobserver.jobs

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import ratpack.exec.Blocking
import ratpack.exec.Promise
import ratpack.jackson.JsonRender
import ratpack.spark.jobserver.SparkConfig
import ratpack.spark.jobserver.SparkJobsConfig
import ratpack.spark.jobserver.containers.ContainersService
import ratpack.spark.jobserver.dto.Result
import ratpack.spark.jobserver.jobs.dto.JobRequest
import ratpack.spark.jobserver.jobs.model.Job
import ratpack.spark.jobserver.jobs.model.JobExecStatus
import ratpack.spark.jobserver.jobs.model.JobsRepository
import ratpack.test.exec.ExecHarness
import ratpack.test.handling.HandlingResult
import ratpack.test.handling.RequestFixture
import spock.lang.AutoCleanup
import spock.lang.IgnoreRest
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
    jobsRepository = Mock(JobsRepository)
    containersService = Mock(ContainersService, constructorArgs: [sparkConfig, sparkJobsConfig])
    jobsService = new JobsService(sparkConfig, sparkJobsConfig, containersService, jobsRepository)
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

  def "required parameters for /jobs/:job_id endpoint has to be provided"() {
    when:
    jobsRepository.findJobs() >> Promise.value(Arrays.asList())
    HandlingResult result = requestFixture.uri("jobs").method("GET").handleChain(jobsEndpoints)

    then:
    result.status.code == 200
    JsonRender jr = result.rendered(JsonRender)
    jr
    jr.object
    jr.object instanceof Result
    Result r = (Result)jr.object
    r
    r.errorCode == "0"
    r.data == []
  }

  def "job not found for /jobs/:job_id"() {
    given:
    jobsRepository.findJob("1") >> Promise.value(null)

    when:
    HandlingResult result = requestFixture.uri("jobs/1").method("GET").handleChain(jobsEndpoints)

    then:
    result.status.code == 200
    JsonRender jr = result.rendered(JsonRender)
    jr
    jr.object
    jr.object instanceof Result
    Result r = (Result)jr.object
    r
    r.errorCode == "IllegalArgumentException"
    r.errorMessage == "Job with id: 1 does not exist"
  }

  def "job found for /jobs/:job_id"() {
    given:
    Job job = new Job("2", JobExecStatus.FINISHED, null)
    jobsRepository.findJob("2") >> Promise.value(job)

    when:
    HandlingResult result = requestFixture.uri("jobs/2").method("GET").handleChain(jobsEndpoints)

    then:
    result.status.code == 200
    JsonRender jr = result.rendered(JsonRender)
    jr
    jr.object
    jr.object instanceof Result
    Result<Job> r = (Result<Job>)jr.object
    r
    r.errorCode == "0"
    r.data == job
  }

  def "get list of jobs"() {
    given:
    List<Job> jobs = [
      Job.of(UUID.fromString("f82eaaa6-c3a0-11e5-9912-ba0be0483c18"), JobExecStatus.WORKING),
      Job.of(UUID.fromString("f82eaee8-c3a0-11e5-9912-ba0be0483c18"), JobExecStatus.FAILED),
      Job.of(UUID.fromString("f82eb00a-c3a0-11e5-9912-ba0be0483c18"), JobExecStatus.FINISHED)
    ]
    jobsRepository.findJobs() >> Promise.value(jobs)

    when:
    def result = requestFixture.uri("jobs").method("GET").handleChain(jobsEndpoints)

    then:
    result.status.code == 200
    JsonRender jr = result.rendered(JsonRender)
    jr
    jr.object
    jr.object instanceof Result
    Result<Collection<Job>> r = (Result<Collection<Job>>)jr.object
    r.errorCode == "0"
    r.data == jobs
  }

  def "get list of jobs - handle error"() {
    given:
    jobsRepository.findJobs() >> { Blocking.get { throw new IOException("IO") } }

    when:
    def result = requestFixture.uri("jobs").method("GET").handleChain(jobsEndpoints)

    then:
    result.status.code == 200
    JsonRender render = result.rendered(JsonRender)
    render.object instanceof Result
    Result r = (Result)render.object
    r.errorCode == "IOException"
  }
}