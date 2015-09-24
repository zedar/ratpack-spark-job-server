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
package ratpack.hadoop.spark.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

/**
 * Result of API call, serializable to {@code JSON}
 */
@AllArgsConstructor
@ToString
@Getter
public class Result<T> {
  private final String errorCode;
  private final String errorMessage;
  private final T value;

  public static <T> Result<T> of(ratpack.exec.Result<T> result) {
    if (result.isSuccess()) {
      return new Result<>("0", null, result.getValue());
    } else {
      return new Result<>(result.getThrowable().getMessage(), result.getThrowable().getCause() != null ? result.getThrowable().getCause().getMessage() : result.getThrowable().getMessage(), null);
    }
  }
}
