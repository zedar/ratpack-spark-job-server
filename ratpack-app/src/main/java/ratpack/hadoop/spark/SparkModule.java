package ratpack.hadoop.spark;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import ratpack.guice.ConfigurableModule;
import ratpack.hadoop.spark.containers.ContainersService;
import ratpack.hadoop.spark.topn.TopNService;

import javax.inject.Singleton;

/**
 * Provides configuration for Apache Spark and Hadoop file system.
 */
public class SparkModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(SparkEndpoints.class).in(Scopes.SINGLETON);
  }

  /**
   * Provides implementation of the {@link TopNService} with {@code Apache Spark}.
   *
   * @param config Apache Spark and HDFS configuration
   * @return the singleton for {@link TopNService} implementation
   */
  @Provides
  @Singleton
  public TopNService topNService(final SparkConfig config, final SparkJobsConfig sparkJobsConfig, final ContainersService containersService) {
    return new TopNService(config, sparkJobsConfig, containersService);
  }
}
