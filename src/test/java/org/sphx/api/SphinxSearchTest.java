package org.sphx.api;

import java.util.Arrays;
import junit.framework.TestCase;

/**
 * {@link SphinxSearch} test.
 *
 * @author Michael Guymon
 */
public class SphinxSearchTest extends TestCase {

  private SphinxSearch search;

  public void setUp() {
    search = new SphinxSearch("testIndex", "testComment");
  }

  public void testAddQuery() {
    search.addQuery( "test" );
    assertEquals("test", search.buildQuery());
  }

  public void testAddFieldQuery() {
    search.addFieldQuery( "testField", "testQuery" );
    assertEquals("@testField \"testQuery\"", search.buildQuery());
  }

  public void testAddFieldsQuery() {
    search.addFieldsQuery( Arrays.asList( new String[] { "testField1", "testField2" } ), "testQuery" );
    assertEquals("@(testField1,testField2) \"testQuery\"", search.buildQuery());
  }
}
