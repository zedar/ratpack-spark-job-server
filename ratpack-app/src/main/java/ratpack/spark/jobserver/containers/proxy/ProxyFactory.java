package ratpack.spark.jobserver.containers.proxy;

import io.netty.util.internal.chmv8.ConcurrentHashMapV8;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A factory for object proxy.
 * The rules:
 * 1. There is a class A with some public interface (methods) on the other classpath (another class loader)
 * 2. There is an interface B that mimics functionality of the target class
 * 3. Proxy is able to load a class A from another classloader by class' name
 * 4. Proxy is able to find methods in class A that correspond to methods from interface B
 * 5. Proxy is able to create an instance of class A hidden within an interface B
 * 6. We are able to call methods of instance of class A throughout the interface B
 */
public class ProxyFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(ProxyFactory.class);

  private static class Handler implements java.lang.reflect.InvocationHandler {
    private final URLClassLoader classLoader;
    private final Object targetObj;
    private final Class targetObjClass;
    private ConcurrentMap<String, Method> cache = new ConcurrentHashMap<>();
    private Function<Class, Function<Object[], Function<String, Method>>> cache2 =
      objClass ->
        methodArgs ->
          Memoizer.memoize(Unchecker.uncheck(methodName -> {
            System.out.println("CALLED FOR: " + methodName + " ARGS: " + methodArgs);
            if (methodArgs != null && methodArgs.length > 0) {
              Class<?>[] methodArgsTypes = Stream.of(methodArgs).map(o -> o.getClass()).toArray(Class[]::new);
              return objClass.getMethod(methodName, methodArgsTypes);
            } else {
              return objClass.getMethod(methodName);
            }
          }));

    public Handler(URLClassLoader classLoader, Object targetObj, Class targetObjClass) {
      this.classLoader = classLoader;
      this.targetObj = targetObj;
      this.targetObjClass = targetObjClass;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
      try {
        Thread.currentThread().setContextClassLoader(classLoader);
        String methodName = method.getName();
        if ("getTarget".equals(methodName)) {
          return targetObj;
        } else {
          Method targetMethod = cache2.apply(targetObjClass).apply(args).apply(methodName);
          /*Method targetMethod = cache.computeIfAbsent(methodName, mn -> uncheckCall(mn, mn1 -> {
            if (args != null && args.length > 0) {
              Class<?>[] types = Stream.of(args).map(o -> o.getClass()).toArray(Class[]::new);
              return targetObjClass.getMethod(mn1, types);
            } else {
              return targetObjClass.getMethod(mn1);
            }
          }));*/
          if (targetMethod != null) {
            return targetMethod.invoke(targetObj, args);
          } else {
            return null;
          }
        }
      } finally {
        Thread.currentThread().setContextClassLoader(classLoader);
      }
    }
  }

  public static <T> T of(Class<T> proxyInterface,
                         URLClassLoader targetObjCL,
                         String targetObjCN) {
    return of(proxyInterface, targetObjCL, targetObjCN, null, null);
  }

  public static <T> T of(Class<T> proxyInterface,
                         URLClassLoader targetObjCL,
                         String targetObjCN,
                         String[] targetObjConstructorParams,
                         Object[] targetObjConstructorArgs) {
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(targetObjCL);
    try {
      Object targetObj = null;
      Class<?> targetObjClass = targetObjCL.loadClass(targetObjCN);
      if (targetObjConstructorParams != null && targetObjConstructorParams.length > 0) {
        Class[] params = Arrays.stream(targetObjConstructorParams)
          .map(cn -> uncheckCall(cn, targetObjCL::loadClass))
          .toArray(Class[]::new);
        Constructor constructor = targetObjClass.getDeclaredConstructor(params);
        targetObj = constructor.newInstance(targetObjConstructorArgs);
      } else {
        targetObj = targetObjClass.newInstance();
      }
      return (T) Proxy.newProxyInstance(
        proxyInterface.getClassLoader(),
        new Class<?>[]{proxyInterface},
        new Handler(targetObjCL, targetObj, targetObjClass));
    } catch (Exception ex) {
      ex.printStackTrace();
      return null;
    } finally {
      Thread.currentThread().setContextClassLoader(classLoader);
    }
  }

  public static URLClassLoader getJarsClassLoader(String rootDir) throws Exception {
    // find urls for all jars necessary for Apache Spark
    ArrayList<URL> urlArrayList = new ArrayList<URL>();
    StringBuilder sb = new StringBuilder();
    Files.walk(Paths.get(rootDir)).sorted().forEach(p -> {
      if (Files.isRegularFile(p)) {
        sb.append(p.getFileName()).append("; ");
        try {
          urlArrayList.add(p.toUri().toURL());
        } catch (Exception ex) {
          throw new RuntimeException(ex);
        }
      }
    });
    // create root class loader
    return URLClassLoader.newInstance(urlArrayList.toArray(new URL[]{}), null);
  }

  public static URLClassLoader getDirClassLoader(Path dir) throws Exception {
    return URLClassLoader.newInstance(new URL[]{dir.toUri().toURL()}, null);
  }

  public static Class loadClass(URLClassLoader targetClassLoader, String className) throws Exception {
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(targetClassLoader);
      return targetClassLoader.loadClass(className);
    } finally {
      Thread.currentThread().setContextClassLoader(classLoader);
    }
  }

  private static <T, V> V uncheckCall(T arg1, ratpack.func.Function<T, V> fn) {
    try {
      return fn.apply(arg1);
    } catch (Throwable t) {
      if (t instanceof RuntimeException) {
        throw (RuntimeException) t;
      } else {
        throw new RuntimeException(t);
      }
    }
  }
}
