package ratpack.spark.jobserver.containers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ratpack.spark.jobserver.SparkConfig;

/**
 * Provides a factory to create Hadoop and Spark Context - the connection to Spark infrastructure as well as to Hadoop's HDFS.
 */
public class SparkCtxService {
  private static final Logger LOGGER = LoggerFactory.getLogger(SparkCtxService.class);

  private static final String SPARK_APP_NAME = "JOBSERVER";

  private final SparkConfig sparkConfig;

  // hadoop configuration shared across all the containers
  private Object hadoopConfiguration;

  // spark configuration used to create JavaSprakContext
  private Object sparkConfiguration;

  // spark context shared across all the containers. Only one SparkContext can exists per JVM
  private Object javaSparkContext;

  /**
   * Constructor used from {@link ContainersModule}
   * @param sparkConfig a configuration of all major spark parameters
   */
  public SparkCtxService(final SparkConfig sparkConfig) {
    this.sparkConfig = sparkConfig;
  }


}
