package org.firebrandocm.dao.utils;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

/**
 * This class starts and stops embedded Cassandra server.
 * <p/>
 * For instance of server is created temporary directory (in target/tmp) for data files and configuration.
 *
 * @author J. Revault d'A...
 */
public class ClassUtil {

  /**
   * Scans all classes accessible from the context class loader which belong to the given package and subpackages.
   *
   * @param packageName The base package
   * @return The classes as a List
   * @throws ClassNotFoundException
   * @throws IOException
   */
  public static List<Class<?>> get( String packageName ) throws ClassNotFoundException, IOException {
    return get( packageName, null );
  }

  /**
   * Scans all classes accessible from the context class loader which belong to the given package and subpackages.
   * Only if the class has the given annotation
   *
   * @param packageName The base package
   * @param  annotation the annotation class we are looking for, null if not used
   * @return The classes as a List
   * @throws ClassNotFoundException
   * @throws IOException
   */
  public static List<Class<?>> get( String packageName, Class<?> annotation ) throws ClassNotFoundException, IOException {
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    assert classLoader != null;
    String path = packageName.replace( '.', '/' );
    Enumeration<URL> resources = classLoader.getResources( path );
    List<File> dirs = new ArrayList<File>();
    while ( resources.hasMoreElements() ) {
      URL resource = resources.nextElement();
      dirs.add( new File( resource.getFile() ) );
    }
    List<Class<?>> classes = new ArrayList<Class<?>>();
    for ( File directory : dirs ) {
      classes.addAll( findClasses( directory, packageName, annotation ) );
    }
    return classes;
  }

  /**
   * Recursive method used to find all classes in a given directory and subdirs.
   *
   * @param directory   The base directory
   * @param packageName The package name for classes found inside the base directory
   * @return The classes as a List
   * @throws ClassNotFoundException
   */
  public static List<Class<?>> findClasses( File directory, String packageName, Class annotation ) throws ClassNotFoundException {
    List<Class<?>> classes = new ArrayList<Class<?>>();
    if ( ! directory.exists() ) {
      return classes;
    }
    File[] files = directory.listFiles();
    if ( files != null ) for ( File file : files ) {
      if ( file.isDirectory() ) {
        assert ! file.getName().contains( "." );
        classes.addAll( findClasses( file, packageName + "." + file.getName(), annotation ) );
      }
      else if ( file.getName().endsWith( ".class" ) ) {

        // TEST added in order to ignore classes names with $
        if ( ! file.getName().contains( "$" ) ) {
          Class<?> clazz = Class.forName( packageName + '.' + file.getName().substring( 0, file.getName().length() - 6 ) );
          if ( annotation != null && clazz.getAnnotation( annotation ) != null ) {
            classes.add( clazz );
          }
        }
      }
    }
    return classes;
  }
}
