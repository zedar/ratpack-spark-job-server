package ratpack.hadoop.spark;

import com.google.common.base.Strings;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * The configuration for {@code SparkModule}. Provide's {@code Apache Spark} settings
 */
@NoArgsConstructor
@ToString
public class SparkConfig {
  private String libsDir;
  private String homeDir;
  private String master;
  private String user;
  private Integer maxCoresPerTask;
  private String fileSystemHost;
  private String fileSystemPort;

  /**
   * The directory with list of all dependencies necessary to execute Apache Spark job.
   * @return the directory with Apache Spark dependencies
   */
  public String getLibsDir() {
    return libsDir;
  }

  /**
   * Sets directory with all Apache Spark dependencies.
   * @param libsDir a directory with libraries/jars
   * @return this
   */
  public SparkConfig libsDir(String libsDir) {
    this.libsDir = libsDir;
    return this;
  }

  /**
   * Apache Spark home directory. Where Spark is installed
   * @return the home directory of Apache Spark
   */
  public String getHomeDir() {
    return homeDir;
  }

  /**
   * Sets Apache Spark home directory.
   * @param homeDir a directory where Apache Spark is installed
   * @return this
   */
  public SparkConfig homeDir(String homeDir) {
    this.homeDir = homeDir;
    return this;
  }

  /**
   * Apache Spark master type.
   * <p>
   * One of: {@code local}, {@code spark://HOST:PORT}, {@code yarn-client}, {@code yarn-cluster}
   * @return the Apache Spark master type
   */
  public String getMaster() {
    return master;
  }

  /**
   * Sets Apache Spark master type
   * @param master a type of Apache Spark master
   * @return this
   */
  public SparkConfig master(String master) {
    this.master = master;
    return this;
  }

  /**
   * Hadoop's users used in map reduce execution.
   * @return the hadoop user
   */
  public String getUser() {
    return user;
  }

  /**
   * Sets hadoop user.
   *
   * @param user the hadoop user
   * @return this
   */
  public SparkConfig user(String user) {
    this.user = user;
    return this;
  }

  /**
   * Maximum number of cores that can be used per one Apache Spark task.
   * <p>
   * If {@code null} unlimited number of cores. But then parallel tasks might be scheduled (executed in FIFO sequence)
   * @return the maximum number of cores
   */
  public Integer getMaxCoresPerTask() {
    return maxCoresPerTask;
  }

  /**
   * Sets maximum number of cores per Apache Spark task.
   * @param maxCoresPerTask maximum number of cores
   * @return this
   */
  public SparkConfig maxCoresPerTask(int maxCoresPerTask) {
    this.maxCoresPerTask = maxCoresPerTask;
    return this;
  }

  /**
   * Hadoop's file system (HDFS) host
   * @return the hadoop file system host
   */
  public String getFileSystemHost() {
    return fileSystemHost;
  }

  /**
   * Sets hadoop file system host
   * @param fileSystemHost the hadoop file system host
   * @return this
   */
  public SparkConfig fileSystemHost(String fileSystemHost) {
    this.fileSystemHost = fileSystemHost;
    return this;
  }

  /**
   * Hadoop's file system (HDFS) port
   * @return the hadoop file system port
   */
  public String getFileSystemPort() {
    return fileSystemPort;
  }

  /**
   * Sets hadoop file system port
   * @param fileSystemPort the hadoop file system port
   * @return this
   */
  public SparkConfig fileSystemPort(String fileSystemPort) {
    this.fileSystemPort = fileSystemPort;
    return this;
  }

  /**
   * Hadoop file system address.
   * @return the address of the hadoop file system
   */
  public String getFileSystemAddress() {
    return "hdfs://" + getFileSystemHost() + (Strings.isNullOrEmpty(getFileSystemPort()) ? "" : ":" + getFileSystemPort());
  }

  /**
   * Get URI to the path in HDFS file system
   * @param fsName the file/dir name
   * @return the URI string
   */
  public String getHDFSURI(String fsName) {
    StringBuilder b = new StringBuilder();
    b.append(this.getFileSystemAddress())
      .append("/user/")
      .append(this.getUser())
      .append("/")
      .append(fsName);
    return b.toString();
  }

  public SparkConfig copyOf(final SparkConfig c) {
    return this
      .libsDir(c.getLibsDir())
      .homeDir(c.getHomeDir())
      .master(c.getMaster())
      .user(c.getUser())
      .maxCoresPerTask(c.getMaxCoresPerTask())
      .fileSystemHost(c.getFileSystemHost())
      .fileSystemPort((c.getFileSystemPort()));
  }
}
