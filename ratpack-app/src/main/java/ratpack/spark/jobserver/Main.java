/*
 * Copyright 2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ratpack.spark.jobserver;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import ratpack.func.Action;
import ratpack.guice.Guice;
import ratpack.handling.RequestLogger;
import ratpack.spark.jobserver.containers.ContainersLifecycle;
import ratpack.spark.jobserver.containers.ContainersModule;
import ratpack.handling.RequestId;
import ratpack.handling.ResponseTimer;
import ratpack.handling.internal.UuidBasedRequestIdGenerator;
import ratpack.server.BaseDir;
import ratpack.server.RatpackServer;
import ratpack.spark.jobserver.jobs.JobsEndpoints;

import java.io.File;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Starting point for the Apacke Spark jobs. Working with hadoop HDFS (distributed file system).
 */
public class Main {
  private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

  public static void main(String... args) throws Exception {
    RatpackServer ratpackServer = RatpackServer.start(spec -> spec
        .serverConfig(builder -> {
            Path basePath = BaseDir.find("application.properties");
            LOGGER.debug("BASE DIR: {}", basePath.toString());
            builder
              .baseDir(BaseDir.find("application.properties"))
              .env()
              .sysProps();

            Path localAppProps = Paths.get("../../config/application.properties");
            if (Files.exists(localAppProps)) {
              LOGGER.debug("LOCALLY OVERLOADED application.properties: {}", localAppProps.toUri().toString());
              builder.props(localAppProps);
            } else {
              URL cpAppProps = Main.class.getClassLoader().getResource("config/application.properties");
              LOGGER.debug("CLASSPATH OVERLOADED application.properties: {}", cpAppProps != null ? cpAppProps.toString() : "DEFAULT LOCATION");
              builder.props(cpAppProps != null ? cpAppProps : Main.class.getClassLoader().getResource("application.properties"));
            }

            Path localSparkJobsProps = Paths.get("../../config/sparkjobs.properties");
            if (Files.exists(localSparkJobsProps)) {
              LOGGER.debug("LOCALLY OVERLOADED sparkjobs.properties: {}", localSparkJobsProps.toUri().toString());
              builder.props(localSparkJobsProps);
            } else {
              URL cpSparkJobsProps = Main.class.getClassLoader().getResource("config/sparkjobs.properties");
              LOGGER.debug("CLASSPATH OVERLOADED SPARKJOBS.PROPS: {}", cpSparkJobsProps != null ? cpSparkJobsProps.toString() : "DEFAULT LOCATION");
              builder.props(cpSparkJobsProps != null ? cpSparkJobsProps : Main.class.getClassLoader().getResource("sparkjobs.properties"));
            }

            builder
              .require("/spark", SparkConfig.class)
              .require("/job", SparkJobsConfig.class);
          }
        )
        .registry(Guice.registry(bindingsSpec -> bindingsSpec
          .bindInstance(ResponseTimer.decorator())
          .module(ContainersModule.class)
          .module(SparkModule.class)
          .bindInstance(new ObjectMapper().writerWithDefaultPrettyPrinter())))
        .handlers(chain -> chain
            .all(ctx -> {
              LOGGER.debug("ALL");
              MDC.put("clientIP", ctx.getRequest().getRemoteAddress().getHostText());
              RequestId.Generator generator = ctx.maybeGet(RequestId.Generator.class).orElse(UuidBasedRequestIdGenerator.INSTANCE);
              RequestId requestId = generator.generate(ctx.getRequest());
              ctx.getRequest().add(RequestId.class, requestId);
              MDC.put("requestId", requestId.toString());
              ctx.next();
            })
            .prefix("v1", chain1 -> chain1
                .all(RequestLogger.ncsa())
                .get("api-def", ctx -> {
                  LOGGER.debug("GET API_DEF.JSON");
                  SparkJobsConfig config = ctx.get(SparkJobsConfig.class);
                  LOGGER.debug("SPARK JOBS CONFIG: " + config.toString());
                  ctx.render(ctx.file("public/apidef/apidef.json"));
                })
                .prefix("spark", JobsEndpoints.class)
            )
        )
    );
    LOGGER.debug("STARTED: {}://{}:{}", ratpackServer.getScheme(), ratpackServer.getBindHost(), ratpackServer.getBindPort());
  }
}
