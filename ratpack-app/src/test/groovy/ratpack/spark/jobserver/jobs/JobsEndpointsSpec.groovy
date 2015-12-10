package ratpack.spark.jobserver.jobs

import ratpack.test.exec.ExecHarness
import spock.lang.AutoCleanup
import spock.lang.Specification

class JobsEndpointsSpec extends Specification {
  @AutoCleanup
  ExecHarness execHarness = ExecHarness.harness()

  JobsService jobsService
  JobsEndpoints jobsEndpoints

  def setup() {
    jobsService = Mock(JobsService)
    jobsEndpoints = new JobsEndpoints(jobsService)
  }

  // use RequestFixture
}