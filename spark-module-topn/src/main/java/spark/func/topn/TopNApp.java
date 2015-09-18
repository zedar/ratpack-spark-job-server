package spark.func.topn;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Calculate the most active {@code N} users out of the access logs.
 */
public class TopNApp {
  private static final Logger LOGGER = LoggerFactory.getLogger(TopNApp.class);

  private static final String USER_REGEX = "(username=')([A-Za-z0-9]{8}+)(')";
  private static final Pattern USER_PATTERN = Pattern.compile(USER_REGEX);

  /**
   * The entry point for {@code Apache Spark} calculation.
   * @param args a list of arguments to initialize spark and hadoop env
   */
  public static void main(String... args) throws Exception{
    if (args.length < 4) {
      throw new RuntimeException("MISSING SPARK CONFIGURATION PARAMETERS");
    }

    String sparkMaster = args[0];
    String sparkMaxCoresPerTask = args[1];
    String inputPath = args[2];
    String outputPath = args[3];

    SparkConf conf = new SparkConf()
      .setAppName("TopN-Users")
      .setMaster(sparkMaster);
    if (sparkMaxCoresPerTask != null && !"".equals(sparkMaxCoresPerTask) && !"*".equals(sparkMaxCoresPerTask)) {
      conf.set("spark.cores.max", sparkMaxCoresPerTask);
    }
    // turn on round robin for resource allocation
    //conf.set("spark.scheduler.mode", "FAIR");
    // setup kryo serializer
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

    JavaSparkContext sparkContext = new JavaSparkContext(conf);

    // TODO:
    runJob(null, sparkContext, null, inputPath, outputPath);

    sparkContext.stop();
  }

  /**
   * Run the job within defined application, it means Spark Context
   * @param sparkContext a Spark Context defined for the {@link SparkConf}
   * @throws Exception any but mainly {@link java.io.IOException}
   */
  public static void runJob(Configuration configuration, JavaSparkContext sparkContext, Map<String, String> params, String inputPath, String outputPath) throws Exception {
    LOGGER.debug("JOB PREPARE OUTPUT");
    if (configuration != null) {
      FileSystem fileSystem = FileSystem.get(configuration);
      fileSystem.delete(new Path(outputPath), false);
    }
    LOGGER.debug("JOB STARTED");
    Integer limit = 10;
    if (params != null) {
      limit = params.get("limit") != null ? Integer.valueOf(params.get("limit")) : limit;
    }
    JavaPairRDD<String, Integer> pairRDD = sparkContext.textFile(inputPath)
      .<String, Integer>flatMapToPair(s -> {
        Matcher matcher = USER_PATTERN.matcher(s);
        if (matcher.find()) {
          return Arrays.asList(new Tuple2<String, Integer>(matcher.group(2), 1));
        } else {
          return Collections.emptyList();
        }
      })
      .reduceByKey(Integer::sum)
      .cache();

    LOGGER.debug("TOTAL COUNT FOR TopN: {}", pairRDD.count());

    List<Tuple2<String, Integer>> result = pairRDD
      .takeOrdered(limit, new ValueComparator());

    sparkContext.parallelizePairs(result).saveAsTextFile(outputPath);
  }

  public static List<List<String>> fetchJobResults(Configuration configuration, String outputPath) throws Exception {
    if (configuration == null) {
      return Lists.newArrayList();
    }
    FileSystem fileSystem = FileSystem.get(configuration);
    FileStatus[] status = fileSystem.listStatus(new Path(outputPath), new PathFilter() {
      @Override
      public boolean accept(Path path) {
        return path.getName().startsWith("part");
      }
    });

    List<List<String>> results = Lists.newArrayList();

    for (int i = 0; i < status.length; i++) {
      FSDataInputStream is = fileSystem.open(status[i].getPath());
      BufferedReader br = new BufferedReader(new InputStreamReader(is));
      String line = br.readLine();
      while(line != null) {
        String[] parts = line.replace("(", "").replace(")", "").split(",");
        if (parts.length >= 2) {
          LOGGER.debug("SPARK RESULT: {} {}", parts[0], parts[1]);
          results.add(Lists.newArrayList(parts[0], parts[1]));
        }
        line = br.readLine();
      }
      is.close();
    }
    return results;
  }

  /**
   * ValueComparator used for soring by value.
   * Has to be serializable in order to use it in {@link JavaSparkContext#parallelizePairs(List)}
   */
  private static class ValueComparator implements Comparator<Tuple2<String, Integer>>, Serializable {
    public ValueComparator() {
    }

    @Override
    public int compare(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) {
      return t2._2().compareTo(t1._2());
    }
  }
}
