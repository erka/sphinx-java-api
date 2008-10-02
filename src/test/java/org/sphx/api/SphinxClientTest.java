package org.sphx.api;

import java.util.HashMap;
import java.util.Map;

import junit.framework.TestCase;
/**
 * To execute these tests you need to execute sphinx_test.sql and configure sphinx using sphinx.conf
 * @author erka
 *
 */
public class SphinxClientTest extends TestCase {

	private SphinxClient sphinxClient;
	protected void setUp() throws Exception {
		super.setUp();
		sphinxClient = new SphinxClient();
	}

	public void testResponseQuery() throws SphinxException {
		SphinxResult result = sphinxClient.Query("wifi", "test1");
		assertEquals(3, result.totalFound);
		assertEquals(3, result.matches.length);
		assertTrue(result.time > -1.0);
		String[] attrNames = result.attrNames;
		assertEquals("created_at", attrNames[0]);
		assertEquals("group_id", attrNames[1]);
		assertEquals(3, result.total);
		SphinxMatch[] matchs = result.matches;
		
		assertEquals(2, matchs[0].docId);
		assertEquals(2, matchs[0].weight);
		assertEquals(new Long(1175658555), matchs[0].attrValues.get(0));
		assertEquals(new Long(2), matchs[0].attrValues.get(1));

		assertEquals(3, matchs[1].docId);
		assertEquals(2, matchs[1].weight);
		assertEquals(new Long(1175658647), matchs[1].attrValues.get(0));
		assertEquals(new Long(1), matchs[1].attrValues.get(1));

		assertEquals(1, matchs[2].docId);
		assertEquals(1, matchs[2].weight);
		assertEquals(new Long(1175658490), matchs[2].attrValues.get(0));
		assertEquals(new Long(1), matchs[2].attrValues.get(1));
		

		SphinxWordInfo sphinxWordInfo = result.words[0];
		assertEquals("wifi", sphinxWordInfo.word);
		assertEquals(6, sphinxWordInfo.hits);
		assertEquals(3, sphinxWordInfo.docs);
	}
	
	
	public void testResponseQueryForRunQuery() throws SphinxException {
		
		int addQuery = sphinxClient.AddQuery("wifi", "test1", "");
		assertEquals(0, addQuery);
		addQuery = sphinxClient.AddQuery("thisstringyouwillneverfound", "test1", "");
		assertEquals(1, addQuery);

		SphinxResult[] results = sphinxClient.RunQueries();
		
		assertEquals(3, results[0].totalFound);
		assertEquals(3, results[0].matches.length);
		SphinxWordInfo sphinxWordInfo = results[0].words[0];
		assertEquals("wifi", sphinxWordInfo.word);
		assertEquals(6, sphinxWordInfo.hits);
		assertEquals(3, sphinxWordInfo.docs);

		assertEquals(0, results[1].totalFound);
		assertEquals(0, results[1].matches.length);
		sphinxWordInfo = results[1].words[0];
		assertEquals("thisstringyouwillneverfound", sphinxWordInfo.word);
		assertEquals(0, sphinxWordInfo.hits);
		assertEquals(0, sphinxWordInfo.docs);

	}
	
	public void testResponseInBuildExcerpts() throws SphinxException {
		String[] docs = {"what the world", "London is the capital of Great Britain"};
		String index = "test1";
		String words = "the";
		Map opts = new HashMap();
		String[] buildExcerpts = sphinxClient.BuildExcerpts(docs, index, words, opts);
		assertNotNull(buildExcerpts);
		assertEquals(2, buildExcerpts.length);
		String[] expected = new String[]{"what <b>the</b> world", "London is <b>the</b> capital of Great Britain"};
		for (int i = 0; i < expected.length; i++) {
			assertEquals(expected[i], buildExcerpts[i]);
		}
	} 
	
	public void testUpdateAttributes() throws SphinxException {
		String[] attrs = {"group_id"};
		long[][] values = new long[1][2];
		values[0][0] = 2;
		values[0][1] = 1;
		
		int updated = sphinxClient.UpdateAttributes("test1", attrs, values);
		assertEquals(1, updated);
		
		SphinxResult result = sphinxClient.Query("wifi", "test1");
		SphinxMatch[] matchs = result.matches;
		assertEquals(2, matchs[0].docId);
		assertEquals(new Long(1), matchs[0].attrValues.get(1));

		values[0][1] = 2;
		updated = sphinxClient.UpdateAttributes("test1", attrs, values);
		assertEquals(1, updated);
		
		result = sphinxClient.Query("wifi", "test1");
		matchs = result.matches;
		assertEquals(2, matchs[0].docId);
		assertEquals(new Long(2), matchs[0].attrValues.get(1));

	}
	
	public void testWrongHostAndPortParameters() {
		
		try {
			sphinxClient.SetServer(null, 10);
			fail();
		}catch (Exception e) {
			assertTrue(true);
			assertEquals("host name must not be empty", e.getMessage());
		}
		
		try {
			sphinxClient.SetServer("", 10);
			fail();
		}catch (Exception e) {
			assertTrue(true);
			assertEquals("host name must not be empty", e.getMessage());
			
		}
		
		try {
			sphinxClient.SetServer("localhost", 0);
			fail();
		}catch (Exception e) {
			assertTrue(true);
			assertEquals("port must be in 1..65535 range", e.getMessage());
			
		}
		try {
			sphinxClient.SetServer("localhost", 65536);
			fail();
		}catch (Exception e) {
			assertTrue(true);
			assertEquals("port must be in 1..65535 range", e.getMessage());
		}
		
		try {
			sphinxClient.SetServer("localhost", 5536);
		}catch (Exception e) {
			fail();
		}
		
		/*
		try {
			new SphinxClient(null, 10);
			fail();
		}catch (Exception e) {
			assertTrue(true);
			assertEquals("host name must not be empty", e.getMessage());
		}
		
		try {
			new SphinxClient("", 10);
			fail();
		}catch (Exception e) {
			assertTrue(true);
			assertEquals("host name must not be empty", e.getMessage());
		}
		
		try {
			new SphinxClient("localhost", 0);
			fail();
		}catch (Exception e) {
			assertTrue(true);
			assertEquals("port must be in 1..65535 range", e.getMessage());
		}
		try {
			new SphinxClient("localhost", 65536);
			fail();
		}catch (Exception e) {
			assertTrue(true);
			assertEquals("port must be in 1..65535 range", e.getMessage());
		}
		*/
	}
	
	public void testConnectToWrongServer() throws SphinxException{
		sphinxClient.SetServer("localhost", 26550);
		sphinxClient.Query("wifi", "test1");
		assertEquals("", sphinxClient.GetLastWarning());
		assertEquals("connection to localhost:26550 failed: java.net.ConnectException: Connection refused", sphinxClient.GetLastError());
	}
	
	
	public void testBuildKeywords() throws SphinxException{
		Map[] buildKeywords = sphinxClient.BuildKeywords("wifi*", "test1", true);
		assertNotNull(buildKeywords);
		assertEquals("wifi", buildKeywords[0].get("tokenized"));
		assertEquals("wifi", buildKeywords[0].get("normalized"));
		assertEquals(new Long(3), buildKeywords[0].get("docs"));
		assertEquals(new Long(6), buildKeywords[0].get("hits"));
		
		buildKeywords = sphinxClient.BuildKeywords("wifi*", "test1", false);
		assertNotNull(buildKeywords);
		assertEquals("wifi", buildKeywords[0].get("tokenized"));
		assertEquals("wifi", buildKeywords[0].get("normalized"));
		assertNull(buildKeywords[0].get("docs"));
		assertNull(buildKeywords[0].get("hits"));
		
	}
}
