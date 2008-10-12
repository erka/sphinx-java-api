/*
 * SphinxClient.java 
 *
 * Java version of Sphinx searchd client (Java API)
 *
 * Copyright (c) 2007-2008, Andrew Aksyonoff
 * Copyright (c) 2007, Vladimir Fedorkov
 * All rights reserved
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License. You should have
 * received a copy of the GPL license along with this program; if you
 * did not, you can find it at http://www.gnu.org/
 */

package org.sphx.api;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.ConnectException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

/** Sphinx client class. */
public class SphinxClient {
	
	private static final int EXCEPTS_WEIGHT_ORDER_FLAG = 16;
	private static final int EXCEPTS_USE_BOUNDARIES_FLAG = 8;
	private static final int EXCEPTS_SINGLE_PASSAGE_FLAG = 4;
	private static final int EXCEPTS_EXACT_PHASE_FLAG = 2;
	
	private static final Integer DEFAULT_EXCEPTS_AROUND = new Integer(5);
	private static final Integer DEFAULT_EXCEPTS_LIMIT = new Integer(256);
	private static final float MILLSEC_IN_SEC = 1000.0f;
	private static final int DEFAULT_LIMIT = 20;
	private static final int DEFAULT_SEARCHD_PORT = 3312;
	public static final long MAX_DWORD = 4294967296L; /*2 ^ 32*/
	
	/* matching modes */
	public static final int SPH_MATCH_ALL			= 0;
	public static final int SPH_MATCH_ANY			= 1;
	public static final int SPH_MATCH_PHRASE		= 2;
	public static final int SPH_MATCH_BOOLEAN		= 3;
	public static final int SPH_MATCH_EXTENDED		= 4;
	public static final int SPH_MATCH_FULLSCAN		= 5;
	public static final int SPH_MATCH_EXTENDED2		= 6;

	/* ranking modes (extended2 only) */
	public static final int SPH_RANK_PROXIMITY_BM25	= 0;
	public static final int SPH_RANK_BM25			= 1;
	public static final int SPH_RANK_NONE			= 2;
	public static final int SPH_RANK_WORDCOUNT		= 3;
	public static final int SPH_RANK_PROXIMITY		= 4;

	/* sorting modes */
	public static final int SPH_SORT_RELEVANCE		= 0;
	public static final int SPH_SORT_ATTR_DESC		= 1;
	public static final int SPH_SORT_ATTR_ASC		= 2;
	public static final int SPH_SORT_TIME_SEGMENTS	= 3;
	public static final int SPH_SORT_EXTENDED		= 4;
	public static final int SPH_SORT_EXPR			= 5;

	/* grouping functions */
	public static final int SPH_GROUPBY_DAY			= 0;
	public static final int SPH_GROUPBY_WEEK		= 1;
	public static final int SPH_GROUPBY_MONTH		= 2;
	public static final int SPH_GROUPBY_YEAR		= 3;
	public static final int SPH_GROUPBY_ATTR		= 4;
	public static final int SPH_GROUPBY_ATTRPAIR	= 5;

	/* searchd reply status codes */
	public static final int SEARCHD_OK				= 0;
	public static final int SEARCHD_ERROR			= 1;
	public static final int SEARCHD_RETRY			= 2;
	public static final int SEARCHD_WARNING			= 3;

	/* attribute types */
	public static final int SPH_ATTR_INTEGER		= 1;
	public static final int SPH_ATTR_TIMESTAMP		= 2;
	public static final int SPH_ATTR_ORDINAL		= 3;
	public static final int SPH_ATTR_BOOL			= 4;
	public static final int SPH_ATTR_FLOAT			= 5;
	public static final int SPH_ATTR_BIGINT			= 6;
	public static final int SPH_ATTR_MULTI			= 0x40000000;


	/* searchd commands */
	public static final int SEARCHD_COMMAND_SEARCH		= 0;
	public static final int SEARCHD_COMMAND_EXCERPT		= 1;
	public static final int SEARCHD_COMMAND_UPDATE		= 2;
	public static final int SEARCHD_COMMAND_KEYWORDS	= 3;
	public static final int SEARCHD_COMMAND_PERSIST		= 4;

	/* searchd command versions */
	public static final int VER_MAJOR_PROTO			= 0x1;
	public static final int VER_COMMAND_SEARCH		= 0x116;
	public static final int VER_COMMAND_EXCERPT		= 0x100;
	public static final int VER_COMMAND_UPDATE		= 0x102;
	public static final int VER_COMMAND_KEYWORDS	= 0x100;

	/* filter types */
	private static final int SPH_FILTER_VALUES = 0;
	private static final int SPH_FILTER_RANGE = 1;
	// TODO FIXME private static final int SPH_FILTER_FLOATRANGE = 2;

	private String		host;
	private int			port;
	private int			offset;
	private int			limit;
	private int			mode;
	private int[]		weights;
	private int			sortMode;
	private String		sortby;
	private int			minId;
	private int			maxId;
	private ByteArrayOutputStream	rawFilters;
	private DataOutputStream		filters;
	private int			filterCount;
	private String		groupBy;
	private int			groupFunc;
	private String		groupSort;
	private String		groupDistinct;
	private int			maxMatches;
	private int			cutoff;
	private int			retryCount;
	private int			retryDelay;
	private String		latitudeAttr;
	private String		longitudeAttr;
	private float		latitude;
	private float		longitude;
	private String		error;
	private String		warning;
	private ArrayList	reqs;
	private Map			indexWeights;
	private int			rankingMode;
	private int			maxQueryTime;
	private Map			fieldWeights;
	private ArrayList	overrides;
	private String 		select;
	
	/** Sphinx client timeout. */
	public static final int SPH_CLIENT_TIMEOUT_MILLISEC	= 30000;
	private static final int DEFAULT_MAX_MATCHES = 1000;
	private static final int MAX_PORT_VALUE = 65536;
	private static final int SPH_MSG_OFFSET = 4;


	/**
	 * Creates a new SphinxClient instance. 
	 */
	public SphinxClient() {
		this("localhost", DEFAULT_SEARCHD_PORT);
	}

	/** 
	 * Creates a new SphinxClient instance, with host:port specification.
	 * @param sphinxHost the host
	 * @param sphinxPort the port
	 */
	public SphinxClient(final String sphinxHost, final int sphinxPort) {
		host = sphinxHost;
		port = sphinxPort;
		offset = 0;
		limit = DEFAULT_LIMIT;
		mode = SPH_MATCH_ALL;
		sortMode = SPH_SORT_RELEVANCE;
		sortby = "";
		minId = 0;
		maxId = 0xFFFFFFFF;

		filterCount = 0;
		rawFilters = new ByteArrayOutputStream();
		filters = new DataOutputStream(rawFilters);

		groupBy = "";
		groupFunc = SPH_GROUPBY_DAY;
		groupSort = "@group desc";
		groupDistinct = "";

		maxMatches = DEFAULT_MAX_MATCHES;
		cutoff = 0;
		retryCount = 0;
		retryDelay = 0;

		latitudeAttr = null;
		longitudeAttr = null;
		latitude = 0;
		longitude = 0;

		error = "";
		warning = "";
		reqs = new ArrayList();
		weights = null;
		indexWeights = new LinkedHashMap();
		fieldWeights = new LinkedHashMap();
		rankingMode = SPH_RANK_PROXIMITY_BM25;
		overrides = new ArrayList();
		select = "*";
	}

	/** 
	 * Get last error message, if any.
	 * @return last warning message
	 * @deprecated {@link SphinxException} will be throw if error happened 
	 */
	public String getLastError() {
		return error;
	}

	/** 
	 * Get last warning message, if any.
	 * @return last warning message 
	 */
	public String getLastWarning() {
		return warning;
	}

	/** 
	 * Set searchd host and port to connect to.
	 * @param sphinxHost the host
	 * @param sphinxPort the port
	 * @throws SphinxException if params are invalid  
	 */
	public void setServer(final String sphinxHost, final int sphinxPort)
			throws SphinxException {
		check(sphinxHost != null && sphinxHost.length() > 0,
				"host name must not be empty");
		check(sphinxPort > 0 && sphinxPort < MAX_PORT_VALUE,
				"port must be in 1..65535 range");
		
		host = sphinxHost;
		port = sphinxPort;
	}

	/** 
	 * Internal method. Sanity check.
	 * @param condition the condition
	 * @param err the error message
	 * @throws SphinxException if condition is false  
	 */
	private void check(final boolean condition, final String err) throws SphinxException {
		if (!condition) {
			error = err;
			throw new SphinxException(err);
		}
	}

	/** 
	 * Internal method. String IO helper.
	 * @param ostream output stream
	 * @param str string to write
	 * @throws IOException if io error occur
	 */
	static void writeNetUTF8(final DataOutputStream ostream, final String str)
			throws IOException {

		ostream.writeShort(0);
		if (str == null) {
			ostream.writeShort(0);
		} else {
			ostream.writeUTF(str);
		}
	}

	/**
	 *  Internal method. String IO helper.
	 *  @param istream input stream
	 *  @throws IOException if io error occur
	 *  @return string from sphinx protocol 
	 */
	static String readNetUTF8(final DataInputStream istream) throws IOException {
		istream.readUnsignedShort(); 
		/*
		 * searchd emits dword lengths, but Java expects words; lets just skip
		 * first 2 bytes
		 */
		return istream.readUTF();
	}

	/**
	 *  Internal method. Unsigned int IO helper.
	 *  @param istream input stream
	 *  @throws IOException if io error occur
	 *  @return value  
	 */
	static long readDword(final DataInputStream istream) throws IOException {
		long v = (long) istream.readInt();
		if (v < 0) {
			v += MAX_DWORD;
		}
		return v;
	}

	/**
	 * Exchange versions with sphinx searchd.
	 * @param sIn input stream
	 * @param sOut output stream 
	 * @throws IOException if IO error occur
	 * @throws SphinxException if invalid version of searchd.
	 */
	private void hello(final DataInputStream sIn, final DataOutputStream sOut)
			throws SphinxException, IOException {
		int version = sIn.readInt();
		if (version < 1) {
			throw new SphinxException(
					"expected searchd protocol version 1+, got version "
							+ version);
		}
		sOut.writeInt(VER_MAJOR_PROTO);
	}

	/**
	 * Close closeable.
	 * @param c the closeable.
	 */
	void close(final Closeable c) {
		if (c != null) {
			try {
				c.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * Get socket to server.
	 * @return the socket
	 * @throws IOException if io error occur.
	 */
	protected Socket getSocket() throws IOException {
		Socket socket = new Socket(host, port);
		socket.setSoTimeout(SPH_CLIENT_TIMEOUT_MILLISEC);
		return socket;
	}

	/**
	 * Internal method. Get and check response packet from searchd.
	 * @param sIn input stream
	 * @throws SphinxException when error occur
	 * @return response data
	 */
	private byte[] response(final DataInputStream sIn) throws SphinxException {

		/* response */
		short status = 0, ver = 0;
		int len = 0;
		
		try {
			/* read status fields */
			status = sIn.readShort();
			ver = sIn.readShort();
			len = sIn.readInt();

			/* read response if non-empty */
			if (len <= 0) {
				throw new SphinxException("invalid response packet size (len="
						+ len + ")");
			}

			byte[] response = new byte[len];
			sIn.readFully(response, 0, len);

			/* check status */
			switch (status) {
				case SEARCHD_OK:
					break;
				case SEARCHD_WARNING:
					DataInputStream in = new DataInputStream(
						new ByteArrayInputStream(response));
					int iWarnLen = in.readInt();
					warning = new String(response, SPH_MSG_OFFSET, iWarnLen);
					System.arraycopy(response, SPH_MSG_OFFSET + iWarnLen, response, 0,
						response.length - SPH_MSG_OFFSET - iWarnLen);
					break;
				case SEARCHD_ERROR:
					throw new SphinxException("searchd error: "
						+ new String(response, SPH_MSG_OFFSET, response.length
								- SPH_MSG_OFFSET));
				case SEARCHD_RETRY:
					throw new SphinxException("temporary searchd error: "
						+ new String(response, SPH_MSG_OFFSET, response.length
								- SPH_MSG_OFFSET));
				default:
					throw new SphinxException(
						"searched returned unknown status, code=" + status);
			}
			
			return response;
			
		} catch (IOException e) {
			String message = "received zero-sized searchd response"
					+ " (searchd crashed?): " + e.getMessage();
			if (len != 0) {
				/* get trace, to provide even more failure details */
				PrintWriter ew = new PrintWriter(new StringWriter());
				e.printStackTrace(ew);
				ew.flush();
				ew.close();
				String sTrace = ew.toString();

				/* build error message */
				message = "failed to read searchd response (status=" + status
						+ ", ver=" + ver + ", len=" + len + ", trace=" + sTrace
						+ ")";
			}
			throw new SphinxException(message);
		}
	}
	

	/**
	 * Internal method. Connect to searchd, send request, get response as
	 * DataInputStream.
	 * 
	 * @param command the command
	 * @param version the clien version
	 * @param req the request data
	 * @throws SphinxException if some error happened.
	 * @return result data stream
	 */
	DataInputStream executeCommand(final int command, final int version,
			final ByteArrayOutputStream req) throws SphinxException {
		/* connect */
		Socket sock = null;
		InputStream sIn = null;
		OutputStream sOut = null;
		try {
			sock = getSocket();
			sIn = sock.getInputStream();
			sOut = sock.getOutputStream();
			DataInputStream dIn = new DataInputStream(sIn);
			DataOutputStream dOut = new DataOutputStream(sOut);
			hello(dIn, dOut);
			request(command, version, req, dOut);
			byte[] data = response(dIn);
			/* spawn that tampon */
			return new DataInputStream(new ByteArrayInputStream(data));
		} catch (ConnectException e) {
			throw new SphinxException("connection to " + host + ":" + port
					+ " failed: " + e);
		} catch (SphinxException e) {
			throw e;
		} catch (Exception e) {
			throw new SphinxException("network error: " + e);
		} finally {
			close(sIn);
			close(sOut);
			close(sock);
		}
	}

	/**
	 * Send request to sphinx.
	 * 
	 * @param command
	 *            the command
	 * @param version
	 *            the version
	 * @param req
	 *            request data
	 * @param dOut
	 *            output stream
	 * @throws IOException
	 *             throw IOException when io error occur.
	 */
	private void request(final int command, final int version,
			final ByteArrayOutputStream req, final DataOutputStream dOut)
			throws IOException {
		
		dOut.writeShort(command);
		dOut.writeShort(version);
		byte[] reqBytes = req.toByteArray();
		dOut.writeInt(reqBytes.length);
		dOut.write(reqBytes);
	}

	/**
	 * Close socket.
	 * @param sock the socket.
	 */
	void close(final Socket sock) {
		if (sock != null) {
			try {
				sock.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * Set matches offset and limit to return to client, max matches to retrieve
	 * on server, and cutoff.
	 * @param offsetValue the offset
	 * @param limitValue the limit 
	 * @param max max matches to retrieve
	 * @param cutoffValue cutoff
	 * @throws SphinxException if invalid value. 
	 */
	public void setLimits(final int offsetValue, final int limitValue,
			final int max, final int cutoffValue) throws SphinxException {
		check(offsetValue >= 0, "offset must not be negative");
		check(limitValue > 0, "limit must be positive");
		check(max > 0, "max must be positive");
		check(cutoffValue >= 0, "cutoff must not be negative");

		offset = offsetValue;
		limit = limitValue;
		maxMatches = max;
		cutoff = cutoffValue;
	}

	/**
	 * Set matches offset and limit to return to client, and max matches to
	 * retrieve on server.
	 * @param offsetValue the offset
	 * @param limitValue the limit 
	 * @param maxValue max matches to retrieve
	 * @throws SphinxException if invalid value. 
	 */
	public void setLimits(final int offsetValue, final int limitValue,
			final int maxValue) throws SphinxException {
		setLimits(offsetValue, limitValue, maxValue, cutoff);
	}

	/** 
	 * Set matches offset and limit to return to client.
	 * @param offsetValue the offset
	 * @param limitValue the limit 
	 * @throws SphinxException if invalid value. 
	 */
	public void setLimits(final int offsetValue, final int limitValue)
			throws SphinxException {
		setLimits(offsetValue, limitValue, maxMatches, cutoff);
	}

	/**
	 * Set maximum query time, in milliseconds, per-index, 0 means "do not
	 * limit".
	 * @param maxTime maximum time for get result
	 * @throws SphinxException if invalid value. 
	 */
	public void setMaxQueryTime(final int maxTime) throws SphinxException {
		check(maxTime >= 0, "max_query_time must not be negative");
		maxQueryTime = maxTime;
	}

	/** 
	 * Set matching mode.
	 * @param modeValue the match mode
	 * @throws SphinxException if invalid value. 
	 */
	public void setMatchMode(final int modeValue) throws SphinxException {
		check(modeValue == SPH_MATCH_ALL || modeValue == SPH_MATCH_ANY
				|| modeValue == SPH_MATCH_PHRASE || modeValue == SPH_MATCH_BOOLEAN
				|| modeValue == SPH_MATCH_EXTENDED || modeValue == SPH_MATCH_EXTENDED2,
				"unknown mode value; use one of the SPH_MATCH_xxx constants");
		mode = modeValue;
	}

	/** 
	 * Set ranking mode.
	 * @param ranker the rank mode
	 * @throws SphinxException if invalid value. 
	 */
	public void setRankingMode(final int ranker) throws SphinxException {
		check(ranker == SPH_RANK_PROXIMITY_BM25 || ranker == SPH_RANK_BM25
				|| ranker == SPH_RANK_NONE || ranker == SPH_RANK_WORDCOUNT
				|| ranker == SPH_RANK_PROXIMITY,
				"unknown ranker value; use one of the SPH_RANK_xxx constants");
		
		rankingMode = ranker;
	}

	/**
	 * Set sorting mode.
	 * @param modeValue the sort mode
	 * @param sortbyValue sort by attribute
	 * @throws SphinxException if invalid value. 
	 */
	public void setSortMode(final int modeValue, final String sortbyValue)
			throws SphinxException {
		
		check(modeValue == SPH_SORT_RELEVANCE
				|| modeValue == SPH_SORT_ATTR_DESC
				|| modeValue == SPH_SORT_ATTR_ASC
				|| modeValue == SPH_SORT_TIME_SEGMENTS
				|| modeValue == SPH_SORT_EXTENDED,
				"unknown mode value; use one of the available SPH_SORT_xxx constants");
		check(modeValue == SPH_SORT_RELEVANCE
				|| (sortbyValue != null && sortbyValue.length() > 0),
				"sortby string must not be empty in selected mode");

		sortMode = modeValue;
		sortby = "";
		if (sortbyValue != null) {
			sortby = sortbyValue;
		}
	}

	/**
	 * Set per-field weights (all values must be positive). WARNING: DEPRECATED,
	 * use SetFieldWeights() instead.
	 * @param weightValues the weights
	 * @throws SphinxException if invalid value.
	 */
	public void setWeights(final int[] weightValues) throws SphinxException {
		check(weightValues != null, "weights must not be null");
		for (int i = 0; i < weightValues.length; i++) {
			int weight = weightValues[i];
			check(weight > 0, "all weights must be greater than 0");
		}
		weights = new int[weightValues.length];
		System.arraycopy(weightValues, 0, weights, 0, weightValues.length);
	}

	/**
	 * Get per-field weights.
	 * 
	 * @return array of values.
	 */
	public int[] getWeights() {
		int[] weightsValue = new int[weights.length];
		System.arraycopy(weights, 0, weightsValue, 0, weights.length);
		return weightsValue;
	}
	
	/**
	 * Bind per-field weights by field name.
	 * 
	 * @param weightValues
	 *            hash which maps String index names to Integer weights
	 * @throws SphinxException if invalid value. 
	 */
	public void setFieldWeights(final Map weightValues) throws SphinxException {
		/* FIXME! implement checks here */
		fieldWeights = new LinkedHashMap();
		if (weightValues != null) {
			fieldWeights.putAll(weightValues);
		}
	}

	/**
	 * Bind per-index weights by index name (and enable summing the weights on
	 * duplicate matches, instead of replacing them).
	 * 
	 * @param weightValues
	 *            hash which maps String index names to Integer weights
	 * @throws SphinxException if invalid value. 
	 */
	public void setIndexWeights(final Map weightValues) throws SphinxException {
		/* FIXME! implement checks here */
		indexWeights =  new LinkedHashMap();
		if (weightValues != null) {
			indexWeights.putAll(weightValues);
		}
	}

	/** 
	 * Set document IDs range to match.
	 * @param min minimum value
	 * @param max maximum value
	 * @throws SphinxException if invalid value. 
	 */
	public void setIDRange(final int min, final int max) throws SphinxException {
		check(min <= max, "min must be less or equal to max");
		minId = min;
		maxId = max;
	}

	/**
	 * Set values filter. Only match records where attribute value is in given
	 * set.
	 * @param attribute the attribute for filter
	 * @param values array of values
	 * @param exclude exclude
	 * @throws SphinxException if invalid value. 
	 */
	public void setFilter(final String attribute, final int[] values,
			final boolean exclude) throws SphinxException {
		check(values != null && values.length > 0,
				"values array must not be null or empty");
		check(attribute != null && attribute.length() > 0,
				"attribute name must not be null or empty");

		try {
			writeNetUTF8(filters, attribute);
			filters.writeInt(SPH_FILTER_VALUES);
			filters.writeInt(values.length);
			for (int i = 0; i < values.length; i++) {
				filters.writeLong(values[i]);
			}
			int excludeValue = 0;
			if (exclude) {
				excludeValue = 1;
			}
			filters.writeInt(excludeValue);

		} catch (Exception e) {
			check(false, "IOException: " + e.getMessage());
		}
		filterCount++;
	}

	/**
	 * Set values filter with a single value (syntax sugar; see
	 * {@link #setFilter(String,int[],boolean)}).
	 * @param attribute the attribute for filter
	 * @param value value value
	 * @param exclude exclude
	 * @throws SphinxException if invalid value. 
	 */
	public void setFilter(final String attribute, final int value,
			final boolean exclude) throws SphinxException {
		
		int[] values = new int[] {value};
		setFilter(attribute, values, exclude);
	}

	/**
	 * Set integer range filter. Only match records if attribute value is
	 * beetwen min and max (inclusive).
	 * @param attribute the attribute for filter
	 * @param min minimum value
	 * @param max maximum value
	 * @param exclude exclude
	 * @throws SphinxException if invalid value or IOException occur. 
	 */
	public void setFilterRange(final String attribute, final int min,
			final int max, final boolean exclude) throws SphinxException {
		check(min <= max, "min must be less or equal to max");
		try {
			writeNetUTF8(filters, attribute);
			filters.writeInt(SPH_FILTER_RANGE);
			filters.writeLong(min);
			filters.writeLong(max);
			int excludeValue = 0;
			if (exclude) {
				excludeValue = 1;
			}
			filters.writeInt(excludeValue);

		} catch (Exception e) {
			check(false, "IOException: " + e.getMessage());
		}
		filterCount++;
	}

	/**
	 * Set float range filter. Only match records if attribute value is beetwen
	 * min and max (inclusive).
	 * @param attribute the attribute for filter
	 * @param min minimum value
	 * @param max maximum value
	 * @param exclude exclude
	 * @throws SphinxException if invalid value or IOException occur.
	 */
	public void setFilterFloatRange(final String attribute, final float min,
			final float max, final boolean exclude) throws SphinxException {

		check(min <= max, "min must be less or equal to max");
		try {
			writeNetUTF8(filters, attribute);
			filters.writeInt(SPH_FILTER_RANGE);
			filters.writeFloat(min);
			filters.writeFloat(max);
			int excludeValue = 0;
			if (exclude) {
				excludeValue = 1;
			}
			filters.writeInt(excludeValue);
		} catch (Exception e) {
			check(false, "IOException: " + e.getMessage());
		}
		filterCount++;
	}

	/**
	 * Setup geographical anchor point. Required to use
	 * @param latitudeAttrValue the latitude attribute
	 * @param longitudeAttrValue the longitude attribute
	 * @param latitudeValue the latitude
	 * @param longitudeValue the longitude
	 * @geodist in filters and sorting; distance will be computed to this point.
	 * @throws SphinxException if invalid value
	 */
	public void setGeoAnchor(final String latitudeAttrValue,
			final String longitudeAttrValue, final float latitudeValue,
			final float longitudeValue) throws SphinxException {
		check(latitudeAttrValue != null && latitudeAttrValue.length() > 0,
				"longitudeAttr string must not be null or empty");
		check(longitudeAttrValue != null && longitudeAttrValue.length() > 0,
				"longitudeAttr string must not be null or empty");

		latitudeAttr = latitudeAttrValue;
		longitudeAttr = longitudeAttrValue;
		latitude = latitudeValue;
		longitude = longitudeValue;
	}

	/**
	 * Set grouping attribute and function.
	 * @param attribute the attribute for group
	 * @param func the function of group process
	 * @param groupSortValue the groupsort
	 * @throws SphinxException if invalid value
	 */
	public void setGroupBy(final String attribute, final int func,
			final String groupSortValue) throws SphinxException {
		check(func == SPH_GROUPBY_DAY || func == SPH_GROUPBY_WEEK
				|| func == SPH_GROUPBY_MONTH || func == SPH_GROUPBY_YEAR
				|| func == SPH_GROUPBY_ATTR || func == SPH_GROUPBY_ATTRPAIR,
				"unknown func value; use one of the available SPH_GROUPBY_xxx constants");

		groupBy = attribute;
		groupFunc = func;
		groupSort = groupSortValue;
	}

	/**
	 * Set grouping attribute and function with default ("@group desc")
	 * groupsort (syntax sugar).
	 * @param attribute the attribute for group
	 * @param func the function of group process
	 * @throws SphinxException if invalid value
	 */
	public void setGroupBy(final String attribute, final int func)
			throws SphinxException {
		setGroupBy(attribute, func, "@group desc");
	}

	/**
	 * Set count-distinct attribute for group-by queries.
	 * @param attribute the attribute for group
	 */
	public void setGroupDistinct(final String attribute) {
		groupDistinct = attribute;
	}

	/**
	 * Set distributed retries count and delay.
	 * @param count how many retries
	 * @param delay what delay between retry
	 * @throws SphinxException if invalid value
	 */
	public void setRetries(final int count, final int delay)
			throws SphinxException {
		check(count >= 0, "count must not be negative");
		check(delay >= 0, "delay must not be negative");
		retryCount = count;
		retryDelay = delay;
	}

	/**
	 * Set distributed retries count with default (zero) delay (syntax sugar).
	 * @param count how many retries
	 * @throws SphinxException if invalid value
	 */
	public void setRetries(final int count) throws SphinxException {
		setRetries(count, 0);
	}

	/** Reset all currently set filters (for multi-queries). */
	public void resetFilters() {
		/* should we close them first? */
		rawFilters = new ByteArrayOutputStream();
		filters = new DataOutputStream(rawFilters);
		filterCount = 0;

		/* reset GEO anchor */
		latitudeAttr = null;
		longitudeAttr = null;
		latitude = 0;
		longitude = 0;
	}

	/**
	 * Connect to searchd server and run current search query against all
	 * indexes (syntax sugar).
	 * @param query the query
	 * @return result from sphinx
	 * @throws SphinxException if error happened.
	 */
	public SphinxResult query(final String query) throws SphinxException {
		return query(query, "*", "");
	}

	/**
	 * Connect to searchd server and run current search query against all
	 * indexes (syntax sugar).
	 * @param query the query
	 * @param index the index name
	 * @return result from sphinx
	 * @throws SphinxException if error happened.
	 */
	public SphinxResult query(final String query, final String index)
			throws SphinxException {
		return query(query, index, "");
	}

	/**
	 *  Connect to searchd server and run current search query.
	 *  @param query the query
	 *  @param index the index name
	 *  @param comment the comment
	 *  @return result from sphinx
	 *  @throws SphinxException if error happened.
	 */
	public SphinxResult query(final String query, final String index, final String comment)
			throws SphinxException {
		check(reqs == null || reqs.size() == 0,
				"AddQuery() and Query() can not be combined; "
				+ "use RunQueries() instead");

		addQuery(query, index, comment);
		SphinxResult[] results = runQueries();
		SphinxResult res = results[0];
		warning = res.warning;
		error = res.error;
		return res;
	}

	/**
	 * Add new query with current settings to current search request.
	 * @param query the query
	 * @param index the index name
	 * @param comment the comment
	 * @throws SphinxException if error happened
	 * @return position query in queries collection.
	 */
	public int addQuery(final String query, final String index, final String comment)
			throws SphinxException {
		ByteArrayOutputStream req = new ByteArrayOutputStream();

		/* build request */
		try {
			DataOutputStream out = new DataOutputStream(req);
			out.writeInt(offset);
			out.writeInt(limit);
			out.writeInt(mode);
			out.writeInt(rankingMode);
			out.writeInt(sortMode);
			writeNetUTF8(out, sortby);
			writeNetUTF8(out, query);
			int weightLen = 0;
			if (weights != null) {
				weightLen = weights.length;
			}

			out.writeInt(weightLen);
			if (weights != null) {
				for (int i = 0; i < weights.length; i++) {
					out.writeInt(weights[i]);
				}
			}

			writeNetUTF8(out, index);
			out.writeInt(0);
			out.writeInt(minId);
			out.writeInt(maxId);

			/* filters */
			out.writeInt(filterCount);
			out.write(rawFilters.toByteArray());

			/* group-by, max matches, sort-by-group flag */
			out.writeInt(groupFunc);
			writeNetUTF8(out, groupBy);
			out.writeInt(maxMatches);
			writeNetUTF8(out, groupSort);

			out.writeInt(cutoff);
			out.writeInt(retryCount);
			out.writeInt(retryDelay);

			writeNetUTF8(out, groupDistinct);

			/* anchor point */
			if (latitudeAttr == null || latitudeAttr.length() == 0
					|| longitudeAttr == null || longitudeAttr.length() == 0) {
				out.writeInt(0);
			} else {
				out.writeInt(1);
				writeNetUTF8(out, latitudeAttr);
				writeNetUTF8(out, longitudeAttr);
				out.writeFloat(latitude);
				out.writeFloat(longitude);

			}

			/* per-index weights */
			out.writeInt(indexWeights.size());
			for (Iterator e = indexWeights.keySet().iterator(); e.hasNext();) {
				String indexName = (String) e.next();
				Integer weight = (Integer) indexWeights.get(indexName);
				writeNetUTF8(out, indexName);
				out.writeInt(weight.intValue());
			}

			/* max query time */
			out.writeInt(maxQueryTime);

			/* per-field weights */
			out.writeInt(fieldWeights.size());
			for (Iterator e = fieldWeights.keySet().iterator(); e.hasNext();) {
				String field = (String) e.next();
				Integer weight = (Integer) fieldWeights.get(field);
				writeNetUTF8(out, field);
				out.writeInt(weight.intValue());
			}

			/* comment */
			writeNetUTF8(out, comment);

			/* attribute overrides */
			out.writeInt(overrides.size());
			
			/* select list */
			writeNetUTF8(out, select);

			
			/* done! */
			out.flush();
			int qIndex = reqs.size();
			reqs.add(qIndex, req.toByteArray());
			return qIndex;

		} catch (IOException e) {
			check(false, "error in AddQuery(): " + e + ": " + e.getMessage());
		} finally {
			try {
				filters.close();
				rawFilters.close();
			} catch (IOException e) {
				check(false, "error in AddQuery(): " + e + ": "
						+ e.getMessage());
			}
		}
		return -1;
	}

	/**
	 * Run all previously added search queries.
	 * @return result from sphinx.
	 * @throws SphinxException if error or no queries for run.
	 */
	public SphinxResult[] runQueries() throws SphinxException {

		check(reqs != null && !reqs.isEmpty(),
				"no queries defined, issue AddQuery() first");

		/* build the mega-request */
		int nreqs = reqs.size();
		ByteArrayOutputStream reqBuf = new ByteArrayOutputStream();
		try {
			DataOutputStream req = new DataOutputStream(reqBuf);
			req.writeInt(nreqs);
			for (int i = 0; i < nreqs; i++) {
				req.write((byte[]) reqs.get(i));
			}
			req.flush();

		} catch (Exception e) {
			throw new SphinxException(
					"internal error: failed to build request: " + e);
		}

		DataInputStream in = executeCommand(SEARCHD_COMMAND_SEARCH,
				VER_COMMAND_SEARCH, reqBuf);
		SphinxResult[] results = new SphinxResult[nreqs];
		reqs = new ArrayList();

		try {
			for (int ires = 0; ires < nreqs; ires++) {
				SphinxResult res = new SphinxResult();
				results[ires] = res;

				int status = in.readInt();
				res.setStatus(status);
				if (status != SEARCHD_OK) {
					String message = readNetUTF8(in);
					if (status == SEARCHD_WARNING) {
						res.warning = message;
					} else {
						res.error = message;
						continue;
					}
				}

				/* read fields */
				int nfields = in.readInt();
				res.fields = new String[nfields];
				// TODO FIXME: int pos = 0;
				for (int i = 0; i < nfields; i++) {
					res.fields[i] = readNetUTF8(in);
				}

				/* read arrts */
				int nattrs = in.readInt();
				res.attrTypes = new int[nattrs];
				res.attrNames = new String[nattrs];
				for (int i = 0; i < nattrs; i++) {
					String attrName = readNetUTF8(in);
					int attrType = in.readInt();
					res.attrNames[i] = attrName;
					res.attrTypes[i] = attrType;
				}

				/* read match count */
				int count = in.readInt();
				int id64 = in.readInt();
				res.matches = new SphinxMatch[count];
				for (int matchesNo = 0; matchesNo < count; matchesNo++) {
					SphinxMatch docInfo;
					boolean isDword = (id64 == 0);
					long curDocId = 0;
					if (isDword) {
						curDocId = readDword(in);
					} else {
						curDocId = in.readLong();
					}
					docInfo = new SphinxMatch(curDocId, in.readInt());

					/* read matches */
					for (int attrNumber = 0; attrNumber < res.attrTypes.length; attrNumber++) {
						// TODO FIXME: String attrName =
						// res.attrNames[attrNumber];
						int type = res.attrTypes[attrNumber];

						/* handle bigints */
						if (type == SPH_ATTR_BIGINT) {
							docInfo.addAttribute(attrNumber, new Long(in
									.readLong()));
							continue;
						}

						/* handle floats */
						if (type == SPH_ATTR_FLOAT) {
							docInfo.addAttribute(attrNumber, new Float(in
									.readFloat()));
							continue;
						}

						/* handle everything else as unsigned ints */
						long val = readDword(in);
						if ((type & SPH_ATTR_MULTI) != 0) {
							long[] vals = new long[(int) val];
							for (int k = 0; k < val; k++) {
								vals[k] = readDword(in);
							}
							docInfo.addAttribute(attrNumber, vals);
						} else {
							docInfo.addAttribute(attrNumber, new Long(val));
						}
					}
					res.matches[matchesNo] = docInfo;
				}

				res.total = in.readInt();
				res.totalFound = in.readInt();
				res.time = in.readInt() / MILLSEC_IN_SEC;

				res.words = new SphinxWordInfo[in.readInt()];
				for (int i = 0; i < res.words.length; i++) {
					res.words[i] = new SphinxWordInfo(readNetUTF8(in),
							readDword(in), readDword(in));
				}
			}
			return results;

		} catch (IOException e) {
			throw new SphinxException("incomplete reply");
		}
	}

	/**
	 * Connect to searchd server and generate excerpts (snippets) from given
	 * documents.
	 * @param docs
	 *            documents
	 * @param index
	 *            index name
	 * @param words
	 *            words
	 * @param userOptions
	 *            maps String keys to String or Integer values (see the
	 *            documentation for complete keys list).
	 * @return null on failure, array of snippets on success.
	 * @throws SphinxException if illegal arguments
	 */
	public String[] buildExcerpts(final String[] docs, final String index,
			final String words, final Map userOptions) throws SphinxException {
		check(docs != null && docs.length > 0,
				"BuildExcerpts: Have no documents to process");
		check(index != null && index.length() > 0,
				"BuildExcerpts: Have no index to process documents");
		check(words != null && words.length() > 0,
				"BuildExcerpts: Have no words to highlight");

		LinkedHashMap opts = new LinkedHashMap();
		if (userOptions != null) {
			opts.putAll(userOptions);
		}

		/* fixup options */
		if (!opts.containsKey("before_match")) {
			opts.put("before_match", "<b>");
		}
		if (!opts.containsKey("after_match")) {
			opts.put("after_match", "</b>");
		}
		if (!opts.containsKey("chunk_separator")) {
			opts.put("chunk_separator", "...");
		}
		if (!opts.containsKey("limit")) {
			opts.put("limit", DEFAULT_EXCEPTS_LIMIT);
		}
		if (!opts.containsKey("around")) {
			opts.put("around", DEFAULT_EXCEPTS_AROUND);
		}
		if (!opts.containsKey("exact_phrase")) {
			opts.put("exact_phrase", new Integer(0));
		}
		if (!opts.containsKey("single_passage")) {
			opts.put("single_passage", new Integer(0));
		}
		if (!opts.containsKey("use_boundaries")) {
			opts.put("use_boundaries", new Integer(0));
		}
		if (!opts.containsKey("weight_order")) {
			opts.put("weight_order", new Integer(0));
		}

		/* build request */
		ByteArrayOutputStream reqBuf = new ByteArrayOutputStream();
		DataOutputStream req = new DataOutputStream(reqBuf);
		try {
			req.writeInt(0);
			int iFlags = 1; /* remove_spaces */
			if (((Integer) opts.get("exact_phrase")).intValue() != 0) {
				iFlags |= EXCEPTS_EXACT_PHASE_FLAG;
			}
			if (((Integer) opts.get("single_passage")).intValue() != 0) {
				iFlags |= EXCEPTS_SINGLE_PASSAGE_FLAG;
			}
			if (((Integer) opts.get("use_boundaries")).intValue() != 0) {
				iFlags |= EXCEPTS_USE_BOUNDARIES_FLAG;
			}
			if (((Integer) opts.get("weight_order")).intValue() != 0) {
				iFlags |= EXCEPTS_WEIGHT_ORDER_FLAG;
			}
			req.writeInt(iFlags);
			writeNetUTF8(req, index);
			writeNetUTF8(req, words);

			/* send options */
			writeNetUTF8(req, (String) opts.get("before_match"));
			writeNetUTF8(req, (String) opts.get("after_match"));
			writeNetUTF8(req, (String) opts.get("chunk_separator"));
			req.writeInt(((Integer) opts.get("limit")).intValue());
			req.writeInt(((Integer) opts.get("around")).intValue());

			/* send documents */
			req.writeInt(docs.length);
			for (int i = 0; i < docs.length; i++) {
				writeNetUTF8(req, docs[i]);
			}
			req.flush();

		} catch (Exception e) {
			throw new SphinxException(
					"internal error: failed to build request: " + e);
		}

		DataInputStream in = executeCommand(SEARCHD_COMMAND_EXCERPT, VER_COMMAND_EXCERPT, reqBuf);
		try {
			String[] res = new String[docs.length];
			for (int i = 0; i < docs.length; i++) {
				res[i] = readNetUTF8(in);
			}
			return res;

		} catch (IOException e) {
			throw new SphinxException("incomplete reply");
		}
	}

	/**
	 * Connect to searchd server and update given attributes on given documents
	 * in given indexes.
	 * 
	 * @param index
	 *            index name(s) to update; might be distributed
	 * @param attrs
	 *            array with the names of the attributes to update
	 * @param values
	 *            array of updates; each long[] entry must contains document ID
	 *            in the first element, and all new attribute values in the
	 *            following ones
	 * @return -1 on failure, amount of actually found and updated documents
	 *         (might be 0) on success
	 *
	 * @throws SphinxException
	 *             on invalid parameters
	 */
	public int updateAttributes(final String index, final String[] attrs,
			final long[][] values) throws SphinxException {
		return updateAttributes(index, attrs, values, false);
	}	
	
	/**
	 * Connect to searchd server and update given attributes on given documents
	 * in given indexes. Sample code that will set group_id=123 where id=1 and
	 * group_id=456 where id=3:
	 *
	 * <pre>
	 * String[] attrs = new String[1];
	 * attrs[0] = &quot;group_id&quot;;
	 * long[][] values = new long[2][2];
	 * values[0] = new long[2];
	 * values[0][0] = 1;
	 * values[0][1] = 123;
	 * values[1] = new long[2];
	 * values[1][0] = 3;
	 * values[1][1] = 456;
	 * int res = cl.UpdateAttributes(&quot;test1&quot;, attrs, values);
	 * </pre>
	 *
	 * @param index
	 *            index name(s) to update; might be distributed
	 * @param attrs
	 *            array with the names of the attributes to update
	 * @param values
	 *            array of updates; each long[] entry must contains document ID
	 *            in the first element, and all new attribute values in the
	 *            following ones
	 * @param mva 
	 * 			 if true value is multi-array value otherwise false
	 * @return -1 on failure, amount of actually found and updated documents
	 *         (might be 0) on success
	 *
	 * @throws SphinxException
	 *             on invalid parameters
	 */
	public int updateAttributes(final String index, final String[] attrs,
			final long[][] values, final boolean mva) throws SphinxException {
		/* check args */
		check(index != null && index.length() > 0, "no index name provided");
		check(attrs != null && attrs.length > 0,
				"no attribute names provided");
		check(values != null && values.length > 0,
				"no update entries provided");

		for (int i = 0; i < values.length; i++) {
			check(values[i] != null, "update entry #" + i + " is null");
			if (mva) {
				check(values[i].length >= 1, "update entry #" + i
						+ " has wrong length");
			} else {
				check(values[i].length == 1 + attrs.length, "update entry #" + i
					+ " has wrong length");
			}
		}

		/* build and send request */
		ByteArrayOutputStream reqBuf = new ByteArrayOutputStream();
		DataOutputStream req = new DataOutputStream(reqBuf);

		try {
			writeNetUTF8(req, index);

			req.writeInt(attrs.length);
			for (int i = 0; i < attrs.length; i++) {
				writeNetUTF8(req, attrs[i]);
				// mva? mutli variables array
				if (mva) {
					req.writeInt(1);
				} else {
					req.writeInt(0);
				}
			}

			req.writeInt(values.length);
			for (int i = 0; i < values.length; i++) {
				/* send docid as 64bit value */
				req.writeLong(values[i][0]);
				if (mva) {
					req.writeInt(values[i].length - 1);
					for (int j = 1; j < values[i].length; j++) {
						req.writeInt((int) values[i][j]);
					}
				} else {
					req.writeInt((int) values[i][1]);
				}
			}

			req.flush();

		} catch (Exception e) {
			throw new SphinxException(
					"internal error: failed to build request: " + e);
		}

		/* get and parse response */
		DataInputStream in = executeCommand(SEARCHD_COMMAND_UPDATE,
				VER_COMMAND_UPDATE, reqBuf);

		try {
			return in.readInt();
		} catch (Exception e) {
			throw new SphinxException("incomplete reply");
		}
	}

	/**
	 * Connect to searchd server, and generate keyword list for a given query.
	 * Returns null on failure, an array of Maps with misc per-keyword info on
	 * success.
	 *
	 * @param query
	 *            the query
	 * @param index
	 *            the index name
	 * @param hits
	 *            include hits statistics
	 * @return build keywords.
	 * @throws SphinxException if error.
	 */
	public Map[] buildKeywords(final String query, final String index,
			final boolean hits) throws SphinxException {

		/* build request */
		ByteArrayOutputStream reqBuf = new ByteArrayOutputStream();
		DataOutputStream req = new DataOutputStream(reqBuf);
		try {
			writeNetUTF8(req, query);
			writeNetUTF8(req, index);
			if (hits) {
				req.writeInt(1);
			} else {
				req.writeInt(0);
			}

		} catch (IOException e) {
			throw new SphinxException(
					"internal error: failed to build request: " + e);
		}

		/* run request */
		DataInputStream in = executeCommand(SEARCHD_COMMAND_KEYWORDS,
				VER_COMMAND_KEYWORDS, reqBuf);
		/* parse reply */
		try {
			int iNumWords = in.readInt();
			Map[] res = new Map[iNumWords];

			for (int i = 0; i < iNumWords; i++) {
				res[i] = new LinkedHashMap();
				res[i].put("tokenized", readNetUTF8(in));
				res[i].put("normalized", readNetUTF8(in));
				if (hits) {
					res[i].put("docs", Long.valueOf(readDword(in)));
					res[i].put("hits", Long.valueOf(readDword(in)));
				}
			}
			return res;

		} catch (IOException e) {
			throw new SphinxException("incomplete reply");
		}
	}
}
