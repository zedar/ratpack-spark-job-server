package ratpack.spark.jobserver.containers.proxy;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

import java.util.Optional;

@Getter
@Setter
@NoArgsConstructor
@ToString
public class SimpleConfig {
  private SimpleConfigAttr attr;

  public SimpleConfig(SimpleConfigAttr attr) {
    this.attr = attr;
  }

  public String getName() {
    return attr != null ? attr.getName() : null;
  }

  public void setName(String name) {
    if (attr == null) {
      attr = new SimpleConfigAttr(name, null);
    } else {
      attr.setName(name);
    }
  }

  public String getValue() {
    return attr != null ? attr.getValue() : null;
  }

  public void setValue(String value) {
    if (attr == null) {
      attr = new SimpleConfigAttr(null, value);
    } else {
      attr.setValue(value);
    }
  }
}
