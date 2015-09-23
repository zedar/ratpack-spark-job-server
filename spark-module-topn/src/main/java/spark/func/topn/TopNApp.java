package spark.func.topn;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
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
  private static final String TIME_AND_USER_REGEX = "^(\\[)([0-9]{2}\\/[A-Za-z]{3}\\/[0-9]{4})(.)+(username=')([A-Za-z0-9]{8}+)(')";
  private static final Pattern TIME_AND_USER_PATTERN = Pattern.compile(TIME_AND_USER_REGEX);

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
    // broadcast the compiled pattern
    Broadcast<Pattern> timeAndUserPattern = sparkContext.broadcast(TIME_AND_USER_PATTERN);

    Integer limit = 10;
    if (params != null) {
      limit = params.get("limit") != null ? Integer.valueOf(params.get("limit")) : limit;
    }
    // broadcast the date from and to across the cluster
    Broadcast<LocalDate> dateFrom = null, dateTo = null;
    if (params != null && !Strings.isNullOrEmpty(params.get("dateFrom"))) {
      LocalDate df = LocalDate.parse(params.get("dateFrom"));
      dateFrom = sparkContext.broadcast(df);
    }
    if (params != null && !Strings.isNullOrEmpty(params.get("dateTo"))) {
      LocalDate dt = LocalDate.parse(params.get("dateTo"));
      dateTo = sparkContext.broadcast(dt);
    }
    Broadcast<LocalDate> dateFromFinal = dateFrom, dateToFinal = dateTo;
    JavaPairRDD<String, Integer> pairRDD = sparkContext.textFile(inputPath)
      .<String, Integer>flatMapToPair(s -> {
        Matcher matcher = timeAndUserPattern.getValue().matcher(s);
        if (matcher.find()) {
          if (dateFromFinal != null || dateToFinal != null) {
            String d = matcher.group(2);
            LocalDate date = LocalDate.parse(d, DateTimeFormatter.ofPattern("dd/MMM/yyyy").withLocale(Locale.ENGLISH));
            if (dateFromFinal != null && date.isBefore(dateFromFinal.getValue())) {
              return Collections.emptyList();
            }
            if (dateToFinal != null && date.isAfter(dateToFinal.getValue())) {
              return Collections.emptyList();
            }
          }
          return Arrays.asList(new Tuple2<String, Integer>(matcher.group(5), 1));
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
