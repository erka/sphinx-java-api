package org.sphx.api;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ConnectException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;

/**
 * To execute these tests you need to execute sphinx_test.sql and configure
 * sphinx using sphinx.conf
 * 
 * @author erka
 * 
 */
public class SphinxClientTest extends TestCase {

	private SphinxClient sphinxClient;
	private SphinxRunner runner = new SphinxRunner(); // static initializers

	// start and stop sphinx
	protected void setUp() throws Exception {
		super.setUp();
		sphinxClient = new SphinxClient("localhost", 43470);
	}

	static public void assertEquals(int[] expected, int[] actual) {
		if (expected.length != actual.length) {
			fail("array don't equals");
		}
		for (int i = 0; i < expected.length; i++) {
			if (expected[i] != actual[i]) {
				fail("array don't equals");
			}
		}
	}

	static public void assertEquals(byte[] expected, byte[] actual) {
		if (expected.length != actual.length) {
			fail("array don't equals");
		}
		for (int i = 0; i < expected.length; i++) {
			if (expected[i] != actual[i]) {
				fail("array don't equals");
			}
		}
	}

	static public void assertEquals(long[] expected, long[] actual) {
		if (expected.length != actual.length) {
			fail("array don't equals");
		}
		for (int i = 0; i < expected.length; i++) {
			if (expected[i] != actual[i]) {
				fail("array don't equals");
			}
		}
	}

	public void testEqualsIntsArray() throws Exception {
		assertEquals(new int[] { -11, 22, 0xFFFF }, new int[] { -11, 22, 0xFFFF });
	}

	public void testEqualsBytesArray() throws Exception {
		assertEquals(new byte[] { 11, 22, 0xf }, new byte[] { 11, 22, 0xf });
	}

	public void testResponseQueryWithNonExistWord() throws SphinxException {
		SphinxResult result = sphinxClient.query("wifi" + System.currentTimeMillis(), "test1");
		assertNotNull(result);
		assertEquals(0, result.totalFound);
		assertEquals(0, result.getStatus());

	}

	public void testResponseQuery() throws SphinxException {
		SphinxResult result = sphinxClient.query("wifi", "test1");

		assertEquals(3, result.totalFound);
		assertEquals(3, result.getMatches().size());
		assertTrue(result.time > -1.0);
		String[] attrNames = result.attrNames;
		assertEquals("created_at", attrNames[0]);
		assertEquals("group_id", attrNames[1]);
		assertEquals(3, result.total);
		List<SphinxMatch> matchs = result.getMatches();

		assertEquals(2, matchs.get(0).getDocId());
		assertEquals(2, matchs.get(0).getWeight());
		assertEquals(getTimeInSeconds("2007-04-04 06:49:15"), matchs.get(0).getAttribute("created_at"));
		assertEquals(new Long(2), matchs.get(0).getAttribute("group_id"));

		assertEquals(3, matchs.get(1).getDocId());
		assertEquals(2, matchs.get(1).getWeight());
		assertEquals(getTimeInSeconds("2007-04-04 06:50:47"), matchs.get(1).getAttribute("created_at"));
		assertEquals(new Long(1), matchs.get(1).getAttribute("group_id"));

		assertEquals(1, matchs.get(2).getDocId());
		assertEquals(1, matchs.get(2).getWeight());
		assertEquals(getTimeInSeconds("2007-04-04 06:48:10"), matchs.get(2).getAttribute("created_at"));
		assertEquals(new Long(1), matchs.get(2).getAttribute("group_id"));

		SphinxWordInfo sphinxWordInfo = result.words[0];
		assertEquals("wifi", sphinxWordInfo.getWord());
		assertEquals(6, sphinxWordInfo.getHits());
		assertEquals(3, sphinxWordInfo.getDocs());
	}

	public void testResponseQueryWrongWay() {
		try {
			int addQuery = sphinxClient.addQuery("wifi", "test1", "");
			assertEquals(0, addQuery);
			sphinxClient.query("test");
			fail();
		} catch (SphinxException e) {
			assertTrue(true);
			assertEquals("AddQuery() and Query() can not be combined; use RunQueries() instead", e
					.getMessage());
		}
	}

	public void testResponseQueryForRunQuery() throws SphinxException {

		int addQuery = sphinxClient.addQuery("wifi", "test1", "");
		assertEquals(0, addQuery);
		addQuery = sphinxClient.addQuery("thisstringyouwillneverfound", "test1", "");
		assertEquals(1, addQuery);

		SphinxResult[] results = sphinxClient.runQueries();

		assertEquals(3, results[0].totalFound);
		assertEquals(3, results[0].getMatches().size());
		SphinxWordInfo sphinxWordInfo = results[0].words[0];
		assertEquals("wifi", sphinxWordInfo.getWord());
		assertEquals(6, sphinxWordInfo.getHits());
		assertEquals(3, sphinxWordInfo.getDocs());

		assertEquals(0, results[1].totalFound);
		assertEquals(0, results[1].getMatches().size());
		sphinxWordInfo = results[1].words[0];
		assertEquals("thisstringyouwillneverfound", sphinxWordInfo.getWord());
		assertEquals(0, sphinxWordInfo.getHits());
		assertEquals(0, sphinxWordInfo.getDocs());

	}

	public void testResponseInBuildExcerpts() throws SphinxException {
		String[] docs = { "what the world", "London is the capital of Great Britain" };
		String index = "test1";
		String words = "the";
		Map opts = new HashMap();
		String[] buildExcerpts = sphinxClient.buildExcerpts(docs, index, words, opts);
		assertNotNull(buildExcerpts);
		assertEquals(2, buildExcerpts.length);
		String[] expected = new String[] { "what <b>the</b> world",
				"London is <b>the</b> capital of Great Britain" };
		for (int i = 0; i < expected.length; i++) {
			assertEquals(expected[i], buildExcerpts[i]);
		}
	}

	public void testUpdateAttributes() throws SphinxException {
		String[] attrs = { "group_id" };
		long[][] values = { { 2, 1 } };

		int updated = sphinxClient.updateAttributes("test1", attrs, values);
		assertEquals(1, updated);

		SphinxResult result = sphinxClient.query("wifi", "test1");
		List<SphinxMatch> matchs = result.getMatches();
		assertEquals(2, matchs.get(0).getDocId());
		assertEquals(new Long(1), matchs.get(0).getAttribute("group_id"));

		values[0][1] = 2;
		updated = sphinxClient.updateAttributes("test1", attrs, values);
		assertEquals(1, updated);

		result = sphinxClient.query("wifi", "test1");
		matchs = result.getMatches();
		assertEquals(2, matchs.get(0).getDocId());
		assertEquals(new Long(2), matchs.get(0).getAttribute("group_id"));
	}

	public void testMVAUpdateAttributes() throws SphinxException {
		long[] mvaOriginal = { 5, 6, 7, 8 };
		long[] mvaForUpdate = { 11, 21 };

		String[] attrs = { "tags" };
		long[][] values = { { 2, 11, 21 } };

		int updated = sphinxClient.updateAttributes("test1", attrs, values, true);
		assertEquals(1, updated);

		SphinxResult result = sphinxClient.query("wifi", "test1");
		List<SphinxMatch> matchs = result.getMatches();
		assertEquals(2, matchs.get(0).getDocId());
		long[] newMva = (long[]) matchs.get(0).getAttribute(3);
		assertEquals(mvaForUpdate, newMva);

		values[0] = new long[] { 2 };
		updated = sphinxClient.updateAttributes("test1", attrs, values, true);
		assertEquals(1, updated);

		result = sphinxClient.query("wifi", "test1");
		matchs = result.getMatches();
		assertEquals(2, matchs.get(0).getDocId());
		newMva = (long[]) matchs.get(0).getAttribute(3);
		assertEquals(new long[0], newMva);

		values[0] = new long[] { 2, 5, 6, 7, 8 };
		updated = sphinxClient.updateAttributes("test1", attrs, values, true);
		assertEquals(1, updated);

		result = sphinxClient.query("wifi", "test1");
		matchs = result.getMatches();
		assertEquals(2, matchs.get(0).getDocId());
		newMva = (long[]) matchs.get(0).getAttribute(3);
		assertEquals(mvaOriginal, newMva);

	}

	public void testUpdateAttributesWithNullAttribute() {
		String[] attrs = { null };
		long[][] values = { { 2, 1 } };

		try {
			sphinxClient.updateAttributes("test1", attrs, values);
			fail();
		} catch (SphinxException e) {
			assertEquals("searchd error: index test1: attribute '' not found", e.getMessage());
		}
	}

	public void testUpdateAttributesWithWrongIndexParam() {

		try {
			sphinxClient.updateAttributes(null, null, null);
			fail();
		} catch (SphinxException e) {
			assertTrue(true);
			assertEquals("no index name provided", e.getMessage());
		}

		try {
			sphinxClient.updateAttributes("", null, null);
			fail();
		} catch (SphinxException e) {
			assertTrue(true);
			assertEquals("no index name provided", e.getMessage());
		}
	}

	public void testUpdateAttributesWithWrongAttrParam() {
		try {
			sphinxClient.updateAttributes("test1", null, null);
			fail();
		} catch (SphinxException e) {
			assertTrue(true);
			assertEquals("no attribute names provided", e.getMessage());
		}
		try {
			sphinxClient.updateAttributes("test1", new String[0], null);
			fail();
		} catch (SphinxException e) {
			assertTrue(true);
			assertEquals("no attribute names provided", e.getMessage());
		}

		/*
		 * TODO try { sphinxClient.UpdateAttributes("test1", new String[1],
		 * null); fail(); } catch (SphinxException e) { assertTrue(true);
		 * assertEquals("no attribute names provided", e.getMessage()); }
		 */
	}

	public void testUpdateAttributesWithWrongValuesParam() {
		String[] strings = new String[] { "group_id", "created_at" };
		try {

			sphinxClient.updateAttributes("test1", strings, null);
			fail();
		} catch (SphinxException e) {
			assertTrue(true);
			assertEquals("no update entries provided", e.getMessage());
		}

		try {
			sphinxClient.updateAttributes("test1", strings, new long[0][0]);
			fail();
		} catch (SphinxException e) {
			assertTrue(true);
			assertEquals("no update entries provided", e.getMessage());
		}

		try {
			long values[][] = { null };
			sphinxClient.updateAttributes("test1", strings, values);
			fail();
		} catch (SphinxException e) {
			assertTrue(true);
			assertEquals("update entry #0 is null", e.getMessage());
		}

		try {
			long values[][] = { { 0, 1 } };
			sphinxClient.updateAttributes("test1", strings, values);
			fail();
		} catch (SphinxException e) {
			assertTrue(true);
			assertEquals("update entry #0 has wrong length", e.getMessage());
		}

		try {
			long values[][] = { { 1, 2 }, {} };
			sphinxClient.updateAttributes("test1", strings, values, true);
			fail();
		} catch (SphinxException e) {
			assertTrue(true);
			assertEquals("update entry #1 has wrong length", e.getMessage());
		}

	}

	public void testWrongHostAndPortParameters() {

		try {
			sphinxClient.setServer(null, 10);
			fail();
		} catch (Exception e) {
			assertTrue(true);
			assertEquals("host name must not be empty", e.getMessage());
		}

		try {
			sphinxClient.setServer("", 10);
			fail();
		} catch (Exception e) {
			assertTrue(true);
			assertEquals("host name must not be empty", e.getMessage());

		}

		try {
			sphinxClient.setServer("localhost", 0);
			fail();
		} catch (Exception e) {
			assertTrue(true);
			assertEquals("port must be in 1..65535 range", e.getMessage());

		}
		try {
			sphinxClient.setServer("localhost", 65536);
			fail();
		} catch (Exception e) {
			assertTrue(true);
			assertEquals("port must be in 1..65535 range", e.getMessage());
		}

		try {
			sphinxClient.setServer("localhost", 5536);
		} catch (Exception e) {
			fail();
		}

		/*
		 * TODO try { new SphinxClient(null, 10); fail(); }catch (Exception e) {
		 * assertTrue(true); assertEquals("host name must not be empty",
		 * e.getMessage()); }
		 * 
		 * try { new SphinxClient("", 10); fail(); }catch (Exception e) {
		 * assertTrue(true); assertEquals("host name must not be empty",
		 * e.getMessage()); }
		 * 
		 * try { new SphinxClient("localhost", 0); fail(); }catch (Exception e)
		 * { assertTrue(true); assertEquals("port must be in 1..65535 range",
		 * e.getMessage()); } try { new SphinxClient("localhost", 65536);
		 * fail(); }catch (Exception e) { assertTrue(true); assertEquals("port
		 * must be in 1..65535 range", e.getMessage()); }
		 */
	}

	public void testConnectToWrongServer() {
		try {
			sphinxClient.setServer("localhost", 26550);
			sphinxClient.query("wifi", "test1");
			fail();
		} catch (SphinxException e) {
			assertEquals(
					"connection to localhost:26550 failed: java.net.ConnectException: Connection refused", e
							.getMessage());
		}
		assertEquals("", sphinxClient.getLastWarning());
	}

	public void testBuildKeywords() throws SphinxException {
		Map[] buildKeywords = sphinxClient.buildKeywords("wifi*", "test1", true);
		assertNotNull(buildKeywords);
		assertEquals("wifi", buildKeywords[0].get("tokenized"));
		assertEquals("wifi", buildKeywords[0].get("normalized"));
		assertEquals(new Long(3), buildKeywords[0].get("docs"));
		assertEquals(new Long(6), buildKeywords[0].get("hits"));

		buildKeywords = sphinxClient.buildKeywords("wifi*", "test1", false);
		assertNotNull(buildKeywords);
		assertEquals("wifi", buildKeywords[0].get("tokenized"));
		assertEquals("wifi", buildKeywords[0].get("normalized"));
		assertNull(buildKeywords[0].get("docs"));
		assertNull(buildKeywords[0].get("hits"));

	}

	public void testSetRetries() {
		try {
			sphinxClient.setRetries(-1);
			fail();
		} catch (SphinxException e) {
			assertEquals("count must not be negative", e.getMessage());
		}
		try {
			sphinxClient.setRetries(1, -1);
			fail();
		} catch (SphinxException e) {
			assertEquals("delay must not be negative", e.getMessage());
		}
		try {
			sphinxClient.setRetries(1, 1);
		} catch (SphinxException e) {
			fail();
		}
	}

	public void testSetGroupBy() {
		try {
			sphinxClient.setGroupBy("group_id", SphinxClient.SPH_ATTR_ORDINAL);
		} catch (SphinxException e) {
			fail();
		}
		try {
			sphinxClient.setGroupBy("group_id", -10002);
			fail();
		} catch (SphinxException e) {
			assertEquals("unknown func value; use one of the available SPH_GROUPBY_xxx constants", e
					.getMessage());
		}

	}

	public void testWriteNetUTF8() throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DataOutputStream ostream = new DataOutputStream(baos);
		SphinxClient.writeNetUTF8(ostream, "test");
		byte[] byteArray = baos.toByteArray();
		byte[] expected = { 0, 0, 0, 4, 116, 101, 115, 116 };
		assertEquals(expected, byteArray);

		baos = new ByteArrayOutputStream();
		ostream = new DataOutputStream(baos);
		SphinxClient.writeNetUTF8(ostream, null);
		byteArray = baos.toByteArray();
		expected = new byte[] { 0, 0, 0, 0 };
		assertEquals(expected, byteArray);

	}

	public void testReadDword() throws IOException {

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DataOutputStream dout = new DataOutputStream(baos);

		dout.writeInt(0);
		dout.writeInt(232220);
		dout.writeInt(-232220);
		dout.writeInt(Integer.MAX_VALUE);
		dout.writeInt(Integer.MIN_VALUE);

		DataInputStream istream = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
		assertEquals(0, SphinxClient.readDword(istream));
		assertEquals(232220, SphinxClient.readDword(istream));
		assertEquals(SphinxClient.MAX_DWORD - 232220, SphinxClient.readDword(istream));
		assertEquals(Integer.MAX_VALUE, SphinxClient.readDword(istream));
		assertEquals(SphinxClient.MAX_DWORD + Integer.MIN_VALUE, SphinxClient.readDword(istream));

	}

	public void testSetLimits() {
		try {
			sphinxClient.setLimits(-1, 1);
			fail();
		} catch (SphinxException e) {
			assertEquals("offset must not be negative", e.getMessage());
		}

		try {
			sphinxClient.setLimits(0, 0, -1);
			fail();
		} catch (SphinxException e) {
			assertEquals("limit must be positive", e.getMessage());
		}

		try {
			sphinxClient.setLimits(1, 1, 0);
			fail();
		} catch (SphinxException e) {
			assertEquals("max must be positive", e.getMessage());
		}

		try {
			sphinxClient.setLimits(1, 1, 1, -1);
			fail();
		} catch (SphinxException e) {
			assertEquals("cutoff must not be negative", e.getMessage());
		}

		try {
			sphinxClient.setLimits(1, 1, 1, 1);
		} catch (SphinxException e) {
			fail();
		}

	}

	public void testSetMaxQueryTime() {
		try {
			sphinxClient.setMaxQueryTime(-1);
			fail();
		} catch (SphinxException e) {
			assertEquals("max_query_time must not be negative", e.getMessage());
		}
		try {
			sphinxClient.setMaxQueryTime(0);
			sphinxClient.setMaxQueryTime(1);
		} catch (SphinxException e) {
			fail();
		}

	}

	public void testHelloWithWrongVersion() throws IOException {
		byte[] hello = new byte[] { 0, 0, 0, 0 };
		final InputStream in = new ByteArrayInputStream(hello);
		final ByteArrayOutputStream out = new ByteArrayOutputStream();
		final Socket socket = new Socket() {
			public InputStream getInputStream() throws IOException {
				return in;
			}

			public OutputStream getOutputStream() throws IOException {
				return out;
			}
		};
		sphinxClient = new SphinxClient() {
			protected Socket getSocket() throws UnknownHostException, IOException {
				return socket;
			}
		};
		ByteArrayOutputStream data = new ByteArrayOutputStream();
		byte[] bs = new byte[] { 0, 2, 3, 4, 5, 3, 2, 3, 4, 4, 3, 3 };
		data.write(bs);
		try {
			sphinxClient.executeCommand(SphinxClient.SEARCHD_COMMAND_UPDATE, SphinxClient.VER_COMMAND_UPDATE,
					data);
			fail();
		} catch (SphinxException e) {
			assertEquals("expected searchd protocol version 1+, got version 0", e.getMessage());
		}
		assertTrue(socket.isClosed());
	}

	public void testConnectFailure() throws IOException {
		final Socket socket = new Socket() {
			public InputStream getInputStream() throws IOException {
				throw new ConnectException("error happened");
			}
		};
		sphinxClient = new SphinxClient() {
			protected Socket getSocket() throws UnknownHostException, IOException {
				return socket;
			}
		};
		ByteArrayOutputStream data = new ByteArrayOutputStream();
		byte[] bs = new byte[] { 0, 2, 3, 4, 5, 3, 2, 3, 4, 4, 3, 3 };
		data.write(bs);
		try {
			sphinxClient.executeCommand(SphinxClient.SEARCHD_COMMAND_UPDATE, SphinxClient.VER_COMMAND_UPDATE,
					data);
			fail();
		} catch (SphinxException e) {
			assertEquals("connection to localhost:3312 failed: java.net.ConnectException: error happened", e
					.getMessage());
		}
		assertTrue(socket.isClosed());
	}

	public void testExecuteCommand() throws IOException, SphinxException {
		byte[] hello = new byte[] { 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 3, 2, 5, 12 };
		final InputStream in = new ByteArrayInputStream(hello);
		final ByteArrayOutputStream out = new ByteArrayOutputStream();
		final Socket socket = new Socket() {
			public InputStream getInputStream() throws IOException {
				return in;
			}

			public OutputStream getOutputStream() throws IOException {
				return out;
			}
		};
		sphinxClient = new SphinxClient() {
			protected Socket getSocket() throws UnknownHostException, IOException {
				return socket;
			}
		};
		ByteArrayOutputStream data = new ByteArrayOutputStream();
		byte[] bs = new byte[] { 0, 2, 3, 4, 5, 3, 2, 3, 4, 4, 3, 3 };
		data.write(bs);

		DataInputStream res = sphinxClient.executeCommand(SphinxClient.SEARCHD_COMMAND_UPDATE,
				SphinxClient.VER_COMMAND_UPDATE, data);
		byte[] response = new byte[3];
		res.readFully(response);
		assertEquals(new byte[] { 2, 5, 12 }, response);

		byte[] expectedBytes = { 0, 0, 0, 1, 0, 2, 01, 02, 0, 0, 0, 12, 0, 2, 3, 4, 5, 3, 2, 3, 4, 4, 3, 3 };
		assertEquals(expectedBytes, out.toByteArray());
		assertTrue(socket.isClosed());
	}

	public void testGetSocket() {
		Socket socket = null;

		try {
			socket = sphinxClient.getSocket();
			assertEquals(SphinxClient.SPH_CLIENT_TIMEOUT_MILLISEC, socket.getSoTimeout());
		} catch (Exception e) {
			fail();
		} finally {
			if (socket != null) {
				try {
					socket.close();
				} catch (IOException e) {
				}
			}
		}
	}

	public void testClose() {
		Closeable c = new Closeable() {
			public void close() throws IOException {
				throw new IllegalArgumentException();
			}

		};
		try {
			sphinxClient.close(c);
			fail();
		} catch (IllegalArgumentException e) {
		}
	}

	public void testRunQueries() {
		try {
			sphinxClient.runQueries();
			fail();
		} catch (SphinxException e) {
			assertEquals("no queries defined, issue AddQuery() first", e.getMessage());
		}
	}

	public void testSetWeights() throws SphinxException {
		int[] weights = { 102, 22, 112, 22, 12, 2 };
		sphinxClient.setWeights(weights);
		assertEquals(weights, sphinxClient.getWeights());
		weights = new int[] { 10, 12, 2 };
		sphinxClient.setWeights(weights);
		assertEquals(weights, sphinxClient.getWeights());
	}

	public void testSetRankingMode() {

		int[] rankingModes = { SphinxClient.SPH_RANK_BM25, SphinxClient.SPH_RANK_NONE,
				SphinxClient.SPH_RANK_PROXIMITY, SphinxClient.SPH_RANK_PROXIMITY_BM25,
				SphinxClient.SPH_RANK_WORDCOUNT };

		for (int i = 0; i < rankingModes.length; i++) {
			try {
				sphinxClient.setRankingMode(rankingModes[i]);
			} catch (SphinxException e) {
				fail();
			}
		}

		try {
			sphinxClient.setRankingMode(-2332);
			fail();
		} catch (SphinxException e) {
			assertTrue(true);
		}
	}

	public void testSetSelectList() {
		try {
			sphinxClient.setSelectList(null);
			fail();
		} catch (SphinxException e) {
			assertEquals("select must be not empty", e.getMessage());
		}

		try {
			sphinxClient.setSelectList("*");
		} catch (SphinxException e) {
			fail(e.getMessage());
		}
	}

	public void testSetOverride() throws SphinxException {
		HashSet overrides = sphinxClient.overrides;
		assertTrue(overrides.isEmpty());
		HashMap hashMap = new HashMap();
		hashMap.put(new Long(1), new Integer(1));
		int[] types = { SphinxClient.SPH_ATTR_TIMESTAMP, SphinxClient.SPH_ATTR_BIGINT,
				SphinxClient.SPH_ATTR_BOOL, SphinxClient.SPH_ATTR_FLOAT, SphinxClient.SPH_ATTR_INTEGER };
		for (int i = 0; i < types.length; i++) {
			sphinxClient.setOverride("n", types[i], hashMap);
		}
		assertFalse(overrides.isEmpty());
		assertEquals(1, overrides.size());
		SphinxOverride override = (SphinxOverride) overrides.iterator().next();
		assertEquals("n", override.getAttrName());
		assertEquals(SphinxClient.SPH_ATTR_INTEGER, override.getAttrType());
		assertSame(hashMap, override.getValues());
		sphinxClient.resetOverrides();
		overrides = sphinxClient.overrides;
		assertTrue(overrides.isEmpty());

	}

	public void testSetOverrideWrongName() {
		HashMap hashMap = new HashMap();
		hashMap.put(new Long(1), new Integer(1));
		try {
			sphinxClient.setOverride(null, SphinxClient.SPH_ATTR_INTEGER, hashMap);
			fail();
		} catch (SphinxException e) {
			assertEquals("attrName must not be empty", e.getMessage());
		}

		try {
			sphinxClient.setOverride("", SphinxClient.SPH_ATTR_INTEGER, hashMap);
			fail();
		} catch (SphinxException e) {
			assertEquals("attrName must not be empty", e.getMessage());
		}
	}

	public void testSetOverrideWrongType() {
		HashMap hashMap = new HashMap();
		hashMap.put(new Long(1), new Integer(1));
		try {
			sphinxClient.setOverride("name", -10, hashMap);
			fail();
		} catch (SphinxException e) {
			assertEquals("unsupported attrType (must be one of INTEGER,"
					+ " TIMESTAMP, BOOL, FLOAT, or BIGINT)", e.getMessage());
		}
	}

	public void testSetOverrideWrongHashMap() {
		HashMap hashMap = new HashMap();
		hashMap.put(new Long(1), new Integer(1));
		try {
			sphinxClient.setOverride("name", SphinxClient.SPH_ATTR_INTEGER, null);
			fail();
		} catch (SphinxException e) {
			assertEquals("values must be not empty", e.getMessage());
		}
	}

	public void testOverride() throws Exception {
		Map values = new HashMap();
		values.put(new Long(2), new Integer(5));
		values.put(new Long(1), new Integer(15));
		sphinxClient.setOverride("group_id", SphinxClient.SPH_ATTR_INTEGER, values);
		SphinxResult result = sphinxClient.query("wifi", "test1");
		List<SphinxMatch> matchs = result.getMatches();
		assertEquals(2, matchs.get(0).getDocId());
		assertEquals(2, matchs.get(0).getWeight());
		assertEquals(getTimeInSeconds("2007-04-04 06:49:15"), matchs.get(0).getAttribute("created_at"));
		assertEquals(new Long(5), matchs.get(0).getAttribute("group_id"));

		assertEquals(3, matchs.get(1).getDocId());
		assertEquals(2, matchs.get(1).getWeight());
		assertEquals(getTimeInSeconds("2007-04-04 06:50:47"), matchs.get(1).getAttribute("created_at"));
		assertEquals(new Long(1), matchs.get(1).getAttribute("group_id"));

		assertEquals(1, matchs.get(2).getDocId());
		assertEquals(1, matchs.get(2).getWeight());
		assertEquals(getTimeInSeconds("2007-04-04 06:48:10"), matchs.get(2).getAttribute("created_at"));
		assertEquals(new Long(15), matchs.get(2).getAttribute("group_id"));
	}

	public void testThrowExpectionForInvalidType() throws SphinxException {

		Map values = new HashMap();
		values.put(new Long(2), new Integer(5));
		values.put(new Long(1), new Integer(15));
		sphinxClient.setOverride("group_id", SphinxClient.SPH_ATTR_BIGINT, values);
		try {
			sphinxClient.query("wifi", "test1");
		} catch (SphinxException e) {
			assertEquals("index test1: attribute override: "
					+ "attribute 'group_id' type mismatch (index=1, query=6)", e.getMessage());
		}
	}

	public Long getTimeInSeconds(String time) {
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		long result = -1;
		try {
			result = simpleDateFormat.parse(time).getTime() / 1000;
		} catch (ParseException e) {
			fail(e.getMessage());
		}
		return Long.valueOf(result);
	}
}
