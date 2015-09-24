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
package spark.func.topn;

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

public class TopNAppTest {
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

      Map<String, String> params = ImmutableMap.of("limit", "3", "dateFrom", "2015-07-12", "dateTo", "2015-07-22");
      TopNApp.runJob(null, sparkContext, params, inputDir, outputDir);

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
