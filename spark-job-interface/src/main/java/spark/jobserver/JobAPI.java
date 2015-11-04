package spark.jobserver;

import com.google.common.base.Strings;
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
  static final String JOBID_ARG = "jobId";
  static final String INPUTDIR_ARG = "inputDir";
  static final String OUTPUTDIR_ARG = "outputDir";

  /**
   * When running Spark job, it is common to prepare some data once for all {@link #runJob(Configuration, JavaSparkContext, Map)} executions.
   * <p>
   * The MLib algorithms can use this method to train the models, store them as reference and reuse it in subsequent {@code runJobs}.
   * @param hadoopConfig a configuration pointing out to hadoop HDFS. It not given a regular file system can be used.
   * @param sparkContext a configuration of the Apache Spark Context
   * @param jobParams a map of parameters including input/output folders
   * @throws Exception any
   */
  default void beforeJob(Configuration hadoopConfig, JavaSparkContext sparkContext, Map<String, String> jobParams) throws Exception {
  }

  /**
   * Runs a Spark job.
   * @param hadoopConfig a configuration pointing out to hadoop HDFS. It not given a regular file system can be used.
   * @param sparkContext a configuration of the Apache Spark Context
   * @param jobParams a map of parameters including input/output folders
   * @throws Exception any
   */
  void runJob(Configuration hadoopConfig, JavaSparkContext sparkContext, Map<String, String> jobParams) throws Exception;

  /**
   * Fetches a Spark job results in the form of list of attributes that have to be interpreted by the caller.
   * @param hadoopConfig a configuration pointing out to hadoop HDFS. It not given a regular file system can be used.
   * @param sparkContext a configuration of the Apache Spark Context
   * @param jobParams a map of parameters including input/output folders
   * @return the list of parameters of type {@code String}
   * @throws Exception any
   */
  default List<List<String>> fetchResults(Configuration hadoopConfig, JavaSparkContext sparkContext, Map<String, String> jobParams) throws Exception {
    return ImmutableList.<List<String>>of();
  }

  /**
   * If there are some resources (like output data folders) allocated by {@link #runJob(Configuration, JavaSparkContext, Map)} they could be released
   * with this method.
   *
   * @param hadoopConfig a configuration pointing out to hadoop HDFS. It not given a regular file system can be used.
   * @param sparkContext
   * @param jobParams
   * @throws Exception
   */
  default void afterJob(Configuration hadoopConfig, JavaSparkContext sparkContext, Map<String, String> jobParams) throws Exception {
  }

  /**
   * Cleans up the allocated resources like RDDs, models.
   * @throws Exception any
   */
  default void cleanUp() throws Exception {
  }

  /**
   * Calculates job's input path, where file to process are located.
   * @param hadoopConfig a configuration of Hadoop's HDFS.
   * @param jobParams job parameters
   * @return string with a full path containing job's input files
   */
  default String getJobInputPath(Configuration hadoopConfig, Map<String, String> jobParams) {
    String inputDir = jobParams.get(INPUTDIR_ARG);
    if (Strings.isNullOrEmpty(inputDir)) {
      return null;
    }
    String hdfs = "";
    if (hadoopConfig != null) {
      hdfs = hadoopConfig.get("fs.defaultFS", "");
    }
    return hdfs + inputDir;
  }

  /**
   * Calculates job's output path, where result data will be stored. If there is {@code JOBID_ARG} given then it is automatically
   * added at the end of the path (with "-" sign before).
   * @param hadoopConfig a configuration of Hadoop's HDFS
   * @param jobParams job parameters
   * @return string with a full path containing job's output files
   */
  default String getJobOutputPath(Configuration hadoopConfig, Map<String, String> jobParams) {
    String jobId = jobParams.get(JOBID_ARG);
    String outputDir = jobParams.get(OUTPUTDIR_ARG);
    if (Strings.isNullOrEmpty(outputDir)) {
      return null;
    }
    String hdfs = "";
    if (hadoopConfig != null) {
      hdfs = hadoopConfig.get("fs.defaultFS", "");
    }
    return hdfs + outputDir + (jobId != null ? "-" + jobId : "");
  }
}
