package spark.jobserver;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.List;
import java.util.Map;

/**
 * An API of a job executed by {@code JobServer}.
 * <p>
 * {@code JobServer} executes Apache Spark jobs.
 */
public interface JobAPI {
  /**
   * When running Spark job, it is common to prepare some data once for all {@link #runJob(Configuration, JavaSparkContext, Map)} executions.
   * <p>
   * The MLib algorithms can use this method to train the models, store them as reference and reuse it in subsequent {@code runJobs}.
   * @param hadoopConfiguration a configuration pointing out to hadoop HDFS. It not given a regular file system can be used.
   * @param sparkContext a configuration of the Apache Spark Context
   * @param jobParams a map of parameters including input/output folders
   * @throws Exception any
   */
  default void beforeJob(Configuration hadoopConfiguration, JavaSparkContext sparkContext, Map<String, String> jobParams) throws Exception {
  }

  /**
   * Runs a Spark job.
   * @param hadoopConfiguration a configuration pointing out to hadoop HDFS. It not given a regular file system can be used.
   * @param sparkContext a configuration of the Apache Spark Context
   * @param jobParams a map of parameters including input/output folders
   * @throws Exception any
   */
  void runJob(Configuration hadoopConfiguration, JavaSparkContext sparkContext, Map<String, String> jobParams) throws Exception;

  /**
   * Fetches a Spark job results in the form of list of attributes that have to be interpreted by the caller.
   * @param hadoopConfiguration a configuration pointing out to hadoop HDFS. It not given a regular file system can be used.
   * @param sparkContext a configuration of the Apache Spark Context
   * @param jobParams a map of parameters including input/output folders
   * @return the list of parameters of type {@code String}
   * @throws Exception any
   */
  default List<List<String>> fetchResults(Configuration hadoopConfiguration, JavaSparkContext sparkContext, Map<String, String> jobParams) throws Exception {
    return ImmutableList.<List<String>>of();
  }

  /**
   * If there are some resources (like output data folders) allocated by {@link #runJob(Configuration, JavaSparkContext, Map)} they could be released
   * with this method.
   *
   * @param hadoopConfiguration a configuration pointing out to hadoop HDFS. It not given a regular file system can be used.
   * @param sparkContext
   * @param jobParams
   * @throws Exception
   */
  default void afterJob(Configuration hadoopConfiguration, JavaSparkContext sparkContext, Map<String, String> jobParams) throws Exception {
  }

  /**
   * Cleans up the allocated resources like RDDs, models.
   * @throws Exception any
   */
  default void cleanUp() throws Exception {
  }
}
