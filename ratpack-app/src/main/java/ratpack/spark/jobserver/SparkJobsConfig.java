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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The configuration for all registered Apache Spark jobs.
 * Point to directories where jars with algorithm definition can be found.
 */
@NoArgsConstructor
@ToString
public class SparkJobsConfig {
  private List<String> jarPaths;
  private List<String> classNames;
  @JsonDeserialize(using = JobsDeserializer.class )
  private Map<String, String> jobs;

  public static class JobsDeserializer extends JsonDeserializer<Map<String, String>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(JobsDeserializer.class);

    @Override
    public Map<String, String> deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
      ObjectCodec oc = p.getCodec();
      String value = oc.readValue(p, String.class);
      LOGGER.debug("JOBS VALUE: {}", value);
      return Splitter.on(",").withKeyValueSeparator("=").split(value);
    }
  }

  /**
   * Map of jo'sb code name to its main class name.
   *
   * @return the map of job's unique code name to job's main class name
   */
  public Map<String, String> getJobs() {
    return jobs;
  }

  /**
   * Sets map of job's code name to its class name. The string should be in form of
   * {@code JOB_NAME=job.class.name,JOB_NAME2=job.class.name2}.
   *
   * @param codeName2className serialized map in form of job name to its class name
   * @return this
   */
  public SparkJobsConfig jobs(String codeName2className) {
    jobs(Splitter.on(",").withKeyValueSeparator("=").split(codeName2className));
    return this;
  }

  /**
   * Sets map of job's code name to its class name
   * @param jobs an immutable map of job's code name to its class name
   * @return this
   */
  public SparkJobsConfig jobs(final Map<String, String> jobs) {
    this.jobs = jobs;
    return this;
  }

  /**
   * Array of paths (directories or directly jars) containing spark jobs and their algorithms
   * @return the array of paths to jars with Spark job implementation
   */
  public String[] getJarPaths() {
    return (jarPaths == null ? Collections.emptyList() : jarPaths).toArray(new String[]{});
  }

  /**
   * Sets an array of paths containing jars with Spark job implementations.
   * @param jarsPaths a list of paths to jars with Spark job implementations
   * @return this
   */
  public SparkJobsConfig jarsPaths(final List<String> jarsPaths) {
    this.jarPaths = jarsPaths;
    return this;
  }

  /**
   * Arrays of full class names implementing registered algorithms
   * @return the arrays of class names
   */
  public String[] getClassNames() {
    List<String> allClassNames = Lists.newArrayList();
    if (classNames != null) {
      allClassNames.addAll(classNames);
    }
    if (jobs != null) {
      allClassNames.addAll(jobs.values());
    }
    return allClassNames.toArray(new String[]{});
  }

  /**
   * Sets an array of class names implementing {@code JobApi} interface
   * @param classNames a list of class names implementing {@code JobApi} interface
   * @return this
   */
  public SparkJobsConfig classNames(final List<String> classNames) {
    if (this.classNames != null) {
      this.classNames.addAll(classNames);
    } else {
      this.classNames = classNames;
    }
    return this;
  }
}
