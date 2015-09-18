package spark.func.topn;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.*;

public class TopNAppTest {
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testTopNApp() {
    Throwable error = null;
    try {
      String inputDir = getClass().getResource("/input").toURI().toString();
      String tempDir = temporaryFolder.getRoot().toURI().toString();
      String outputDir = temporaryFolder.getRoot().toURI().toString() + "output";

      assertTrue(temporaryFolder.getRoot().exists());
      Path path = Paths.get(temporaryFolder.getRoot().toString());
      assertNotNull(path.toFile());
      assertTrue(path.toFile().exists());

      TopNApp.main("local", null, inputDir, outputDir);

      // Read output file
      path = Paths.get(temporaryFolder.getRoot().toString());
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
    } catch (Exception ex) {
      ex.printStackTrace();
      error = ex;
    }
    assertNull(error);
  }
}
