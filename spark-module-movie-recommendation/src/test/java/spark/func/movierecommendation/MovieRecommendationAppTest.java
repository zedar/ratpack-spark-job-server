package spark.func.movierecommendation;

import com.google.common.collect.ImmutableMap;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

import static org.junit.Assert.*;

public class MovieRecommendationAppTest {
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  public static JavaSparkContext sparkContext;

  @BeforeClass
  public static void setup() {
    SparkConf conf = new SparkConf()
      .setAppName("TopNApp")
      .setMaster("local")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.io.compression.codec", "lz4");
    sparkContext = new JavaSparkContext(conf);
  }

  @AfterClass
  public static void teardown() {
    if (sparkContext != null) {
      sparkContext.stop();
      sparkContext = null;
    }
  }

  @Test
  public void testMovieRecommendation() throws Exception {
    String inputDir = getClass().getResource("/ml-latest-small").toURI().toString();
    String tempDir = temporaryFolder.getRoot().toURI().toString();
    String outputDir = temporaryFolder.getRoot().toURI().toString() + "output";

    Map<String, String> params = ImmutableMap.of("limit", "3", "userId", "1");

    MovieRecommendationApp.runJob(null, sparkContext, params, inputDir, outputDir);

    // Read output file
    Path path = Paths.get(temporaryFolder.getRoot().toString());
    assertNotNull(path.toFile());
    path = Paths.get(temporaryFolder.getRoot().toString() + "/output");
    assertNotNull(path.toFile());
    assertTrue(path.toFile().exists());

    Files.walk(path)
      .forEach(p -> {
        if (Files.isDirectory(p)) {
          return;
        }
        // {@code p.getFileName()} return the {@code Path}. The method {@code startsWith()} checks full file names
        if (p.getFileName().toString().startsWith("part")) {
          try {
            System.out.println(p.toUri().toString());
            System.out.println(new String(Files.readAllBytes(p)));
          } catch (IOException ex) {
            throw new RuntimeException(ex);
          }
        }
      });

    //assertNotNull(null);
  }
}
