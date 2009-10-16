package org.sphx.api;

import java.util.Arrays;
import junit.framework.TestCase;

/**
 * {@link ExtendedSphinxClient} test
 *
 * To execute these tests you need to execute sphinx_test.sql and configure
 * sphinx using sphinx.conf
 * 
 * @author Michael Guymon
 */
public class ExtendedSphinxClientTest extends TestCase {

  private ExtendedSphinxClient sphinxClient;
	private SphinxRunner runner = new SphinxRunner(); // static initializers
 
	// start and stop sphinx
	protected void setUp() throws Exception {
		super.setUp();
		sphinxClient = new ExtendedSphinxClient("localhost", 43470);
	}

  public void testAddFieldSearch() throws SphinxException {
    assertEquals( 0, sphinxClient.addFieldQuery( "name", "GSM", "test1", "ExtendedSphinxClientTest#testAddFieldSearch" ) );
    assertEquals( 1, sphinxClient.addFieldQuery( "name", "comply", "test1", "ExtendedSphinxClientTest#testAddFieldSearch" ) );
    assertEquals( 2, sphinxClient.addFieldQuery( "description", "comply", "test1", "ExtendedSphinxClientTest#testAddFieldSearch" ) );

    SphinxResult[] results = sphinxClient.runQueries();

    // Match GSM in the Name field, expects 1 match
    assertEquals(1, results[0].totalFound);
		assertEquals(1, results[0].matches.length);

    SphinxWordInfo sphinxWordInfo = results[0].words[0];
		assertEquals("gsm", sphinxWordInfo.getWord());
		assertEquals(3, sphinxWordInfo.getHits()); // XXX: word count should be 1?
		assertEquals(1, sphinxWordInfo.getDocs());

    // Match comply in the Name field, expects no match
    assertEquals(0, results[1].totalFound);
		assertEquals(0, results[1].matches.length);

    sphinxWordInfo = results[1].words[0];
		assertEquals("comply", sphinxWordInfo.getWord());
		assertEquals(1, sphinxWordInfo.getHits()); // XXX: word count should be 0?
		assertEquals(1, sphinxWordInfo.getDocs());

    // Match comply in the Description field, expects no match
    assertEquals(1, results[2].totalFound);
		assertEquals(1, results[2].matches.length);

    sphinxWordInfo = results[2].words[0];
		assertEquals("comply", sphinxWordInfo.getWord());
		assertEquals(1, sphinxWordInfo.getHits()); 
		assertEquals(1, sphinxWordInfo.getDocs());
  }

  public void testAddFieldsSearch() throws SphinxException {
    assertEquals( 0, sphinxClient.addFieldsQuery( Arrays.asList( new String[] { "name", "description" } ), "to", "test1", "ExtendedSphinxClientTest#testAddFieldsSearch" ) );

    SphinxResult[] results = sphinxClient.runQueries();

    // Match GSM in the Name field, expects 1 match
    assertEquals(2, results[0].totalFound);
		assertEquals(2, results[0].matches.length);

    SphinxWordInfo sphinxWordInfo = results[0].words[0];
		assertEquals("to", sphinxWordInfo.getWord());
		assertEquals(4, sphinxWordInfo.getHits());
		assertEquals(2, sphinxWordInfo.getDocs());

  }
}
