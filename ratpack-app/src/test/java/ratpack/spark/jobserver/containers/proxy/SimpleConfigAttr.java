package ratpack.spark.jobserver.containers.proxy;

import lombok.*;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class SimpleConfigAttr {
  private String name;
  private String value;
}
