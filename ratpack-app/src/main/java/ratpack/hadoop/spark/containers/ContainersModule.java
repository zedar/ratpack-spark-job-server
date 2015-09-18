package ratpack.hadoop.spark.containers;

import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
import ratpack.guice.ConfigurableModule;

/**
 * Provides services for working with containers able to execute Apache Spark jobs.
 */
public class ContainersModule extends AbstractModule {
  @Override
  protected void configure() {
    bind(ContainersService.class).in(Scopes.SINGLETON);
  }
}
