package ratpack.spark.jobserver.containers.proxy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * A factory for object proxy.
 * The rules:
 * 1. Assume that there is a class A with some public interface (methods).
 * 2. This class has to be loaded with seperate class loader.
 * 3. There is an interface B that mimics functionality of the target class A. It is available on the main classloader.
 * 3. Proxy is able to load a class A with another classloader by its full class name.
 * 4. Proxy is able to find methods in class A that correspond to methods from interface B
 * 5. Proxy is able to create an instance of class A hidden within an interface B
 * 6. We are able to call methods of instance of class A throughout the interface B
 */
public class ProxyObjectThroughInterface {
  private static final Logger LOGGER = LoggerFactory.getLogger(ProxyObjectThroughInterface.class);

  /**
   * Invocation handler that proxies interface methods calls to the target class instance.
   * The handler is able to cache method definitions.
   */
  private static class Handler implements java.lang.reflect.InvocationHandler {
    // Target class loader, where proxied class is loaded on
    private final URLClassLoader classLoader;
    // An instance of target class
    private final Object targetObj;
    // A target object class definition
    private final Class targetObjClass;
    // Cache of method signature to method class instance.
    // The method signature is composed of tuple: class definition, method name, list of classes for method attributes
    private Function<Tuple3<Class,String,Class[]>, Method> cache = Memoizer.memoize(Unchecker.uncheck(key -> {
      if (key._2() != null && key._2().length > 0) {
        return key._0().getMethod(key._1(), key._2());
      } else {
        return key._0().getMethod(key._1());
      }
    }));
    // Converted of tuple: class definition, method name, list of method arguments to the method declaration
    private Function<Tuple3<Class,String,Object[]>, Tuple3<Class,String,Class[]>> mapMethodDecl = key ->
      key.map(c -> c, name -> name, args -> Optional.ofNullable(args).map(args2 -> Stream.of(args2).map(o -> o.getClass()).toArray(Class[]::new)).orElse(null));

    /**
     * The invocation handler for object loaded from another class loader.
     * @param classLoader a class loader for the target object
     * @param targetObj a target object instance
     * @param targetObjClass a target object class definition
     */
    public Handler(URLClassLoader classLoader, Object targetObj, Class targetObjClass) {
      this.classLoader = classLoader;
      this.targetObj = targetObj;
      this.targetObjClass = targetObjClass;
    }

    /**
     * Invoke method on the target object. There is one special method {@code getTarget} that returns target object itself.
     * @param proxy an instance of proxy to the target object
     * @param method a method to invoke
     * @param args list of method arguments
     * @return the return value from target object method call
     * @throws Throwable any
     */
    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
      try {
        Thread.currentThread().setContextClassLoader(classLoader);
        String methodName = method.getName();
        if ("getTarget".equals(methodName)) {
          return targetObj;
        } else {
          Method targetMethod = cache.apply(mapMethodDecl.apply(Tuple.of(targetObjClass, methodName, args)));
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

  /**
   * Create {@link Proxy} object that implements {@code proxyInterface} contract. The target object is loaded with
   * {@code targetObjectCL} class loader. The full name of proxied class is defined by {@code targetObjCN} attribute.
   * @param proxyInterface an interface with declaration of target object methods
   * @param targetObjCL a class loader that should be used to load target object
   * @param targetObjCN a full class name of the target object
   * @param <T> a type of proxy interface
   * @return {@link Proxy} to the target object
   */
  public static <T> T of(Class<T> proxyInterface,
                         URLClassLoader targetObjCL,
                         String targetObjCN) {
    return of(proxyInterface, targetObjCL, targetObjCN, null, null);
  }

  /**
   * Create {@link Proxy} object with the constructor that has a list of {@code targetObjConstructorParams}
   * @param proxyInterface n interface with declaration of methods provided by target object
   * @param targetObjCL a class loader that should be used to load target class
   * @param targetObjCN a full name of target class
   * @param targetObjConstructorParams list of names of target object constructor parameters
   * @param targetObjConstructorArgs list of arguments for target object constructor
   * @param <T> a type of proxy interface
   * @return {@link Proxy} to the target object
   */
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

  /**
   * Build class loader with jars that are located in the {@code rootDir}. Walk through the directory tree.
   * @param rootDir a root directory for jars to be included in the output class loader.
   * @return the class loader with all jars from {@code rootDir} included in class path.
   * @throws Exception any
   */
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

  /**
   * Build class loader from the path. Classes that are unpacked are automatically included in class path.
   * @param dir a directory with classes
   * @return the class loader
   * @throws Exception any
   */
  public static URLClassLoader getDirClassLoader(Path dir) throws Exception {
    return URLClassLoader.newInstance(new URL[]{dir.toUri().toURL()}, null);
  }

  /**
   * Load class with the {@code targetClassLoader}.
   * @param targetClassLoader a class loader where class is located
   * @param className a full class name
   * @return the found class definition
   * @throws Exception any
   */
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
