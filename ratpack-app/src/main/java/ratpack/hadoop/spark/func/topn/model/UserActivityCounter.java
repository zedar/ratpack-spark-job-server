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
package ratpack.hadoop.spark.func.topn.model;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import java.util.List;

/**
 * User activity counter. Counts all requests sent by a user.
 */
@Getter
@AllArgsConstructor
@ToString
@EqualsAndHashCode
public class UserActivityCounter {
  private final String username;
  private final Integer counter;

  public static UserActivityCounter of(String... args) {
    if (args.length < 1) {
      return null;
    } else if (args.length < 2) {
      return new UserActivityCounter(args[0], null);
    } else {
      return new UserActivityCounter(args[0], Integer.valueOf(args[1]));
    }
  }

  public static UserActivityCounter of(List<String> args) {
    return of(args.toArray(new String[]{}));
  }
}
