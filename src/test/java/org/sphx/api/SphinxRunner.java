package org.sphx.api;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sphx.util.JUnitProperties;

/**
 * Helper to automatically start Sphinx's searchd before the tests 
 * run and stop searchd after the tests are finished.
 *
 * @see {@link JUnitProperties}
 * @author Michael Guymon
 */
public class SphinxRunner {

  private static Logger logger = LoggerFactory.getLogger(SphinxRunner.class);
  private static JUnitProperties junitProperties = new JUnitProperties();
  static {
    try {
      start();

      Thread hook = new Thread( new ShutdownSphinx() );
      Runtime.getRuntime().addShutdownHook(hook); // shuts down searchd when VM exits

    } catch (IOException ex) {
      logger.error("Failed to start sphinx", ex);
    }    
  }

  /**
   * Start Sphinx
   *
   * @throws IOException
   */
  private static void start() throws IOException {

    String sphinxDataPath = "target/sphinx/data/log";

    File sphinxDataDir = new File(sphinxDataPath);
    if (!sphinxDataDir.exists()) {
      if (!sphinxDataDir.mkdirs()) {
        throw new IOException("Failed to create sphinx data directory " + sphinxDataPath);
      }
    }

    shutdown();

    // indexer -c src/test/resources/sphinx.conf test1
    String[] command = new String[]{ junitProperties.getSphinxIndexer(), "-c", "src/test/resources/sphinx.conf", "test1", "test2"};
    logger.debug("Running indexer: {}", StringUtils.join( command, " " ) );
    execute( command );
    
    // searchd -c src/test/resources/sphinx.conf -p 4347
    command = new String[]{ junitProperties.getSphinxSearcherd(), "-c", "src/test/resources/sphinx.conf" };
    logger.debug("Running searchd: {}", StringUtils.join( command, " " ) );
    execute( command );
  }


  /**
   * Shutdown Sphinx
   */
  private static void shutdown() {
      String[] command = new String[]{ junitProperties.getSphinxSearcherd(), "-c", "src/test/resources/sphinx.conf", "--stop"};
      logger.debug("Stopping searchd: {}", StringUtils.join( command, " " ) );
      try {
        // searchd -c src/test/resources/sphinx.conf -p 4347 --stop
        SphinxRunner.execute( command );
      } catch (Exception ex) {
        logger.error( "Failed to shutdown searchd", ex );
      }
  }

  /**
   * Execute a command line process
   *
   * @param command String[]
   * @throws IOException
   */
  private static void execute( String[] command ) throws IOException {
    Process process = null;
    process = Runtime.getRuntime().exec(command);

    BufferedReader stdInput = new BufferedReader(new InputStreamReader(process.getInputStream()));

    BufferedReader stdError = new BufferedReader(new InputStreamReader(process.getErrorStream()));

    String output = null;
    while ((output = stdInput.readLine()) != null) {
      logger.debug(output);
    }

    // read any errors from the attempted command
    while ((output = stdError.readLine()) != null) {
      logger.debug(output);
    }

  }

  /**
   * Executed {@link Runnable} when VM exists to stop Sphinx
   */
  private static class ShutdownSphinx implements Runnable {
    private static Logger logger = LoggerFactory.getLogger(ShutdownSphinx.class);
    
    public void run() {
      logger.debug( "Running exit hook" );
      shutdown();
    }
  }
}
