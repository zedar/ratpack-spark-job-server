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
import ratpack.guice.Guice;
import ratpack.spark.jobserver.containers.ContainersLifecycle;
import ratpack.spark.jobserver.containers.ContainersModule;
import ratpack.handling.RequestId;
import ratpack.handling.ResponseTimer;
import ratpack.handling.internal.UuidBasedRequestIdGenerator;
import ratpack.server.BaseDir;
import ratpack.server.RatpackServer;

/**
 * Starting point for the Apacke Spark jobs. Working with hadoop HDFS (distributed file system).
 */
public class Main {
  private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

  public static void main(String... args) throws Exception {
    RatpackServer ratpackServer = RatpackServer.start(spec -> spec
        .serverConfig(builder -> builder
            .baseDir(BaseDir.find("application.properties"))
            .props(Main.class.getClassLoader().getResource("application.properties"))
            .props(Main.class.getClassLoader().getResource("sparkjobs.properties"))
            .env().sysProps()
            .require("/spark", SparkConfig.class)
            .require("/job", SparkJobsConfig.class)
        )
        .registry(Guice.registry(bindingsSpec -> {
          bindingsSpec
            .bindInstance(ResponseTimer.decorator())
            .module(ContainersModule.class)
            .module(SparkModule.class)
            .bindInstance(new ObjectMapper().writerWithDefaultPrettyPrinter())
            .bind(ContainersLifecycle.class);
        }))
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
                .get("api-def", ctx -> {
                  LOGGER.debug("GET API_DEF.JSON");
                  SparkJobsConfig config = ctx.get(SparkJobsConfig.class);
                  LOGGER.debug("JOBS PATHS: " + config.getJarPaths());
                  ctx.render(ctx.file("public/apidef/apidef.json"));
                })
                .prefix("spark", SparkEndpoints.class)
            )
        )
    );
    LOGGER.debug("STARTED: {}://{}:{}", ratpackServer.getScheme(), ratpackServer.getBindHost(), ratpackServer.getBindPort());
  }
}
