package ratpack.hadoop.spark;

import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * The configuration for all registered Apache Spark jobs.
 * Point to directories where jars with algorithm definition can be found.
 */
@NoArgsConstructor
@ToString
public class SparkJobsConfig {
  private String topNJarsDir;
  private String movieRecommendationJarsDir;

  /**
   * The path to folder with jars containing TopN algorithm. In Apache Spark convention
   * @return the path to TopN jars
   */
  public String getTopNJarsDir() {
    return topNJarsDir;
  }

  /**
   * Sets the path to TopN jars folder.
   * @param topNJars the path
   * @return this
   */
  public SparkJobsConfig topNJarsDir(String topNJars) {
    this.topNJarsDir = topNJars;
    return this;
  }

  /**
   * The path to folder with jars or one jar implementing movie recommendation algorithm.
   * @return tge path to jars
   */
  public String getMovieRecommendationJarsDir() {
    return movieRecommendationJarsDir;
  }

  /**
   * Sets the path to the jars implementing movie recommendations algorithm
   * @param movieRecommendationJarsDir a path
   * @return this
   */
  public SparkJobsConfig movieRecommendationJarsDir(String movieRecommendationJarsDir) {
    this.movieRecommendationJarsDir = movieRecommendationJarsDir;
    return this;
  }
}
