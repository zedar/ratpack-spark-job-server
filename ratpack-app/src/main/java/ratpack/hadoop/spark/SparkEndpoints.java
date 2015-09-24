package ratpack.hadoop.spark;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ratpack.func.Action;
import ratpack.hadoop.spark.dto.Result;
import ratpack.hadoop.spark.func.movierecommendation.MovieRecommendationService;
import ratpack.hadoop.spark.func.movierecommendation.dto.Request;
import ratpack.hadoop.spark.func.movierecommendation.model.MovieRecommendation;
import ratpack.hadoop.spark.func.topn.TopNService;
import ratpack.hadoop.spark.func.topn.dto.CalcTopN;
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
  private final MovieRecommendationService movieRecommendationService;

  @Inject
  public SparkEndpoints(final TopNService topNService, final MovieRecommendationService movieRecommendationService) {
    this.topNService = topNService;
    this.movieRecommendationService = movieRecommendationService;
  }

  @Override
  public void execute(Chain chain) throws Exception {
    chain
      .path("top/:n?", ctx -> {
        Integer topN = Integer.valueOf(ctx.getPathTokens().getOrDefault("n", "10"));
        ctx.byMethod(byMethodSpec -> byMethodSpec
            .post(() -> {
              ctx.parse(fromJson(CalcTopN.class))
                .onNull(() -> ctx.render(Integer.valueOf(-1)))
                .then(ctn -> {
                  topNService
                    .apply2(null, ctn.getLimit(), ctn.getTimeInterval(), "input", "output")
                    .map(Result::of)
                    .map(r -> json(r))
                    .then(ctx::render);
                });
          })
        );
      })
      .path("top2", ctx -> {
        Integer topN = Integer.valueOf(ctx.getPathTokens().getOrDefault("n", "10"));
        ctx.byMethod(byMethodSpec -> byMethodSpec
            .post(() -> {
              ctx.parse(fromJson(CalcTopN.class))
                .onNull(() -> ctx.render(Integer.valueOf(-1)))
                .then(ctn -> {
                  topNService
                    .apply2("TopN2", ctn.getLimit(), ctn.getTimeInterval(), "input", "output")
                    .map(Result::of)
                    .map(r -> json(r))
                    .then(ctx::render);
                });
            })
        );
      })
      .path("movies", ctx -> {
        ctx.byMethod(byMethodSpec -> byMethodSpec
            .post(() -> ctx
                .parse(fromJson(Request.class))
                .onNull(() -> ctx.render(Integer.valueOf(-1)))
                .then(req -> movieRecommendationService
                    .apply(req, "input_movie", "output_movie")
                    .map(Result::of)
                    .map((r -> json(r)))
                    .then(ctx::render)
                )
            )
        );
      });
  }
}
