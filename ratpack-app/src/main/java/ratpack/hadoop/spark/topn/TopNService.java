package ratpack.hadoop.spark.topn;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ratpack.exec.Blocking;
import ratpack.exec.Promise;
import ratpack.exec.Result;
import ratpack.hadoop.spark.SparkConfig;
import ratpack.hadoop.spark.SparkJobsConfig;
import ratpack.hadoop.spark.containers.ContainersService;
import ratpack.hadoop.spark.topn.model.Limit;
import ratpack.hadoop.spark.topn.model.TimeInterval;
import ratpack.hadoop.spark.topn.model.UserActivityCounter;
import ratpack.hadoop.spark.util.ClassUtil;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Executes Top-N algorithm with Apache Spark
 */
public class TopNService {
  private final Logger LOGGER = LoggerFactory.getLogger(TopNService.class);

  private final SparkConfig config;
  private final SparkJobsConfig sparkJobsConfig;
  private final ContainersService containersService;

  public TopNService(final SparkConfig config, final SparkJobsConfig sparkJobsConfig, final ContainersService containersService) {
    this.config = config;
    this.sparkJobsConfig = sparkJobsConfig;
    this.containersService = containersService;
  }

  /**
   * Executes job for calculating top-n users by their activity count.
   * <p>
   * If {@code timeInterval} is provided calculation is executed between {@code dateFrom} and {@code dateTo}
   * @param limit a limit for the number of the most active users
   * @param timeInterval a time interval for looking for the most active users
   * @param inputFS a hadoop file system where user activity logs are stored
   * @param outputFS a hadoop file system where calculation results are stored
   * @return the promise for the result of top-n map-reduce calculation
   */
  public Promise<Result<List<UserActivityCounter>>> apply2(Limit limit, TimeInterval timeInterval, String inputFS, String outputFS) {
    return containersService
      .getJobContainer("TopN", sparkJobsConfig.getTopNJarsDir(), "spark.func.topn.TopNApp")
      .flatMap(container -> {
        String inputPath = config.getHDFSURI(Strings.isNullOrEmpty(inputFS) ? "input" : inputFS);
        String outputPath = config.getHDFSURI(Strings.isNullOrEmpty(outputFS) ? "output" : outputFS);
        ImmutableMap<String, String> params = ImmutableMap.of(
          "limit", limit.getValue().toString(),
          "dateFrom", (timeInterval != null && timeInterval.getDateFrom() != null ? timeInterval.getDateFrom().toString() : ""),
          "dateTo", (timeInterval != null && timeInterval.getDateTo() != null ? timeInterval.getDateTo().toString() : ""));
        return container
          .runJob(params, inputPath, outputPath)
          .flatMap(uuid -> container.<List<List<String>>>fetchJobResults(outputPath, uuid));
      })
      .map(results -> {
        ArrayList<UserActivityCounter> userActivityCounters = Lists.newArrayList();
        if (results != null) {
          for (List<String> result : results) {
            UserActivityCounter userActivityCounter = UserActivityCounter.of(result);
            LOGGER.debug("RESULT ENTRY: {}", userActivityCounter.toString());
            userActivityCounters.add(userActivityCounter);
          }
        }
        return ImmutableList.copyOf(userActivityCounters);
      })
      .map(Result::success);
  }

  public Promise<Result<List<UserActivityCounter>>> apply(Limit limit, TimeInterval timeInterval, String inputFS, String outputFS) {
    String uuid = "-" + UUID.randomUUID().toString();
    String jobId = "top-n-users" + uuid;
    return Blocking.get(() -> {
      LOGGER.debug("SPARK JOB INIT jobId={}", jobId);

      ClassLoader threadClassLoader = Thread.currentThread().getContextClassLoader();

      try {
        ArrayList<URL> urlArrayList = new ArrayList<URL>();
        Files.walk(Paths.get(config.getLibsDir())).forEach(p -> {
          if (Files.isRegularFile(p)) {
            try {
              urlArrayList.add(p.toUri().toURL());
            } catch (Exception ex) {
              throw new RuntimeException(ex);
            }
          }
        });
        Files.walk(Paths.get(sparkJobsConfig.getTopNJarsDir())).forEach(p -> {
          if (Files.isRegularFile(p)) {
            try {
              urlArrayList.add(p.toUri().toURL());
            } catch (Exception ex) {
              throw new RuntimeException(ex);
            }
          }
        });
        URLClassLoader urlClassLoader = URLClassLoader.newInstance(urlArrayList.toArray(new URL[]{}), null);

        Thread.currentThread().setContextClassLoader(urlClassLoader);

        LOGGER.debug("Module class name: {}", this.getClass().getName());
        String moduleJarPath = ClassUtil.findContainingJar(urlClassLoader, "spark.func.topn.TopNApp");
        LOGGER.debug("Module JAR Path: {}", moduleJarPath);

        Class configurationClass = urlClassLoader.loadClass("org.apache.hadoop.conf.Configuration");
        Object configuration = configurationClass.newInstance();
        Method method = configurationClass.getMethod("set", String.class, String.class);
        method.invoke(configuration, "fs.defaultFS", config.getFileSystemAddress());

        String inputPath = config.getHDFSURI(Strings.isNullOrEmpty(inputFS) ? "input" : inputFS);
        String outputPath = config.getHDFSURI((Strings.isNullOrEmpty(outputFS) ? "output" : outputFS) + uuid);

        Class sparkConfClass = urlClassLoader.loadClass("org.apache.spark.SparkConf");
        Object sparkConf = sparkConfClass.newInstance();
        Method m = sparkConfClass.getMethod("setAppName", String.class);
        m.invoke(sparkConf, "TopN-Users");
        m = sparkConfClass.getMethod("setMaster", String.class);
        m.invoke(sparkConf, config.getMaster());
        m = sparkConfClass.getMethod("setJars", new Class[]{String[].class});
        m.invoke(sparkConf, new Object[]{new String[]{moduleJarPath}});
        String maxCoresPerTask = config.getMaxCoresPerTask() == null ? "2" : config.getMaxCoresPerTask().toString();
        m = sparkConfClass.getMethod("set", String.class, String.class);
        m.invoke(sparkConf, "spark.cores.max", maxCoresPerTask);
        m.invoke(sparkConf, "spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        m.invoke(sparkConf, "spark.io.compression.codec", "lz4");

        Class javaSparkContextClass = urlClassLoader.loadClass("org.apache.spark.api.java.JavaSparkContext");
        Constructor jscConstructor = javaSparkContextClass.getDeclaredConstructor(sparkConfClass);
        Object javaSparkContext = jscConstructor.newInstance(sparkConf);
        Class topnAppClass = urlClassLoader.loadClass("spark.func.topn.TopNApp");
        Method runJobMethod = topnAppClass.getMethod("runJob", configurationClass, javaSparkContextClass, String.class, String.class);

        runJobMethod.invoke(null, configuration, javaSparkContext, inputPath, outputPath);

        m = javaSparkContextClass.getMethod("stop");
        m.invoke(javaSparkContext);

        LOGGER.debug("SPARK JOB EXECUTION END jobId={}", jobId);
        LOGGER.debug("SPARK JOB START COLLECTING RESULTS jobId={}", jobId);

        ArrayList<UserActivityCounter> userActivityCounters = Lists.newArrayList();
        Method getJobResultsMethod = topnAppClass.getMethod("getJobResults", configurationClass, String.class);
        if (getJobResultsMethod != null) {
          List<List<String>> results = (List<List<String>>)getJobResultsMethod.invoke(null, configuration, outputPath);
          if (results != null) {
            for (List<String> result : results) {
              UserActivityCounter userActivityCounter = UserActivityCounter.of(result);
              LOGGER.debug("RESULT ENTRY: {}", userActivityCounter.toString());
              userActivityCounters.add(userActivityCounter);
            }
          }
        }
        LOGGER.debug("SPARK JOB END job={}", jobId);
        return Result.success(ImmutableList.copyOf(userActivityCounters));
      } catch (Exception ex) {
        ex.printStackTrace();
        return Result.error(new RuntimeException(ex));
      } finally {
        Thread.currentThread().setContextClassLoader(threadClassLoader);
      }
    });
  }
}
