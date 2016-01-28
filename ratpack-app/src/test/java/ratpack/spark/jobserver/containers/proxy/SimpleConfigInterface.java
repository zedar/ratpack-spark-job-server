package ratpack.spark.jobserver.containers.proxy;

public interface SimpleConfigInterface extends ProxyInterface {
  void setName(String name);
  void setValue(String value);
  String getName();
  String getValue();
  String toString();
  void setAttr(Object attr);
}
