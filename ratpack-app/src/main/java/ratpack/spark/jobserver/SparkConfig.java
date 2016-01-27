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
package ratpack.spark.jobserver;

import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;

/**
 * The configuration for {@code SparkModule}. Provide's {@code Apache Spark} settings
 */
@Getter
@Setter
@Accessors(chain = true)
@NoArgsConstructor
@ToString
public class SparkConfig {
  /**
   * The directory with list of all dependencies necessary to execute Apache Spark job.
   * @param libsDir a directory with libraries/jars
   * @return the directory with Apache Spark dependencies
   */
  private String libsDir;
  /**
   * Apache Spark home directory. Where Spark is installed
   * @param homeDir a directory where Apache Spark is installed
   * @return the home directory of Apache Spark
   */
  private String homeDir;
  /**
   * Apache spark configuration paramter {@code spark.executor.extraJavaOptions}
   * @param extraJavaOptions Apache Spark parameter {@code spark.executor.extraJavaOptions}
   * @return Apache Spark configuration parameter {@code spark.executor.extraJavaOptions}
   */
  private String extraJavaOptions;
  /**
   * Apache Spark master type.
   * <p>
   * One of: {@code local}, {@code spark://HOST:PORT}, {@code yarn-client}, {@code yarn-cluster}
   * @param master a type of Apache Spark master
   * @return the Apache Spark master type
   */
  private String master;
  /**
   * Hadoop's users used in map reduce execution.
   * @param user the hadoop user
   * @return the hadoop user
   */
  private String user;
  /**
   * Maximum number of cores that can be used per one Apache Spark task.
   * <p>
   * If {@code null} unlimited number of cores. But then parallel tasks might be scheduled (executed in FIFO sequence)
   * @param maxCoresPerTask maximum number of cores
   * @return the maximum number of cores
   */
  private Integer maxCoresPerTask;
  /**
   * Hadoop's file system (HDFS) host
   * @param fileSystemHost the hadoop file system host
   * @return the hadoop file system host
   */
  private String fileSystemHost;
  /**
   * Hadoop's file system (HDFS) port
   * @param fileSystemPort the hadoop file system port
   * @return the hadoop file system port
   */
  private String fileSystemPort;
  /**
   * {@code es.nodes} - List of Elasticsearch nodes to connect to. Each node can have its own port specified: {@code mynode:9200}
   * OPTIONAL: true
   * @param esNodes list of elasticsearch nodes to connect to
   * @return the string with list of elastic search nodes to connect to
   */
  private String esNodes;

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
  public String hdfsURI(String fsName) {
    StringBuilder b = new StringBuilder();
    b.append(this.getFileSystemAddress())
      .append("/user/")
      .append(this.getUser())
      .append("/")
      .append(fsName);
    return b.toString();
  }
}
