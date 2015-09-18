package ratpack.hadoop.spark.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLDecoder;
import java.util.Enumeration;

public class ClassUtil {
  private final static Logger LOGGER = LoggerFactory.getLogger(ClassUtil.class);

  /**
   * Find a jar that contains a class of the same name, if any.
   * It will return a jar file, even if that is not the first thing
   * on the class path that has a class with the same name.
   *
   * @param urlClassLoader class loader to look for a class. If it is not given {@code clazz.getClassLoader()}
   * @param className the name of the class to find.
   * @return a jar file that contains the class, or null.
   * @throws RuntimeException runtime exception
   */
  public static String findContainingJar(URLClassLoader urlClassLoader, String className) {
    ClassLoader loader = null;
    if (urlClassLoader != null) {
      loader = urlClassLoader;
    } else {
      loader = ClassUtil.class.getClassLoader();
    }
    String classFile = className.replaceAll("\\.", "/") + ".class";
    LOGGER.debug("CLASSFILE: {}", classFile);
    try {
      for(final Enumeration<URL> itr = loader.getResources(classFile); itr.hasMoreElements();) {
        final URL url = itr.nextElement();
        if ("jar".equals(url.getProtocol())) {
          String toReturn = url.getPath();
          if (toReturn.startsWith("file:")) {
            toReturn = toReturn.substring("file:".length());
          }
          toReturn = URLDecoder.decode(toReturn, "UTF-8");
          return toReturn.replaceAll("!.*$", "");
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return null;
  }
}
