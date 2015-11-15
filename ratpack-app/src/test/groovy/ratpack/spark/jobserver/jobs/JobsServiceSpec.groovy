package ratpack.spark.jobserver.jobs

import ratpack.spark.jobserver.SparkConfig
import ratpack.test.exec.ExecHarness
import spock.lang.AutoCleanup
import spock.lang.Specification

class JobsServiceSpec extends Specification {
  @AutoCleanup
  ExecHarness execHarness = ExecHarness.harness()

  def setup() {
    SparkConfig sparkConfig = new SparkConfig()
  }
}