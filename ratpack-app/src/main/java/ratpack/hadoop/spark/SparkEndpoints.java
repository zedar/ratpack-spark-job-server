package ratpack.hadoop.spark;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ratpack.func.Action;
import ratpack.hadoop.spark.dto.Result;
import ratpack.hadoop.spark.topn.TopNService;
import ratpack.hadoop.spark.topn.dto.CalcTopN;
import ratpack.handling.Chain;

import javax.inject.Inject;

import static ratpack.jackson.Jackson.fromJson;
import static ratpack.jackson.Jackson.json;

/**
 * {@code /spark} endpoints chain. Executes {@code Apache Spark} algorithms
 */
public class SparkEndpoints implements Action<Chain> {
  private static final Logger LOGGER = LoggerFactory.getLogger(SparkEndpoints.class);

  private final TopNService topNService;

  @Inject
  public SparkEndpoints(final TopNService topNService) {
    this.topNService = topNService;
  }

  @Override
  public void execute(Chain chain) throws Exception {
    chain
      .path("top/:n?", ctx -> {
        LOGGER.debug("IN SPARK ENDPOINTS");
        Integer topN = Integer.valueOf(ctx.getPathTokens().getOrDefault("n", "10"));
        ctx.byMethod(byMethodSpec -> byMethodSpec
            .post(() -> {
              LOGGER.debug("IN SPARK ENDPOINTS 2");
              ctx.parse(fromJson(CalcTopN.class))
                .onNull(() -> ctx.render(Integer.valueOf(-1)))
                .then(ctn -> {
                  topNService
                    .apply2(ctn.getLimit(), ctn.getTimeInterval(), "input", "output")
                    .map(Result::of)
                    .map(r -> json(r))
                    .then(ctx::render);
                });
          })
        );
      });
  }
}
