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
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

/** Sphinx client class */
public class SphinxClient {
	
	public final static long MAX_DWORD = 4294967296L; /*2 ^ 32*/
	/* matching modes */
	public final static int SPH_MATCH_ALL			= 0;
	public final static int SPH_MATCH_ANY			= 1;
	public final static int SPH_MATCH_PHRASE		= 2;
	public final static int SPH_MATCH_BOOLEAN		= 3;
	public final static int SPH_MATCH_EXTENDED		= 4;
	public final static int SPH_MATCH_FULLSCAN		= 5;
	public final static int SPH_MATCH_EXTENDED2		= 6;

	/* ranking modes (extended2 only) */
	public final static int SPH_RANK_PROXIMITY_BM25	= 0;
	public final static int SPH_RANK_BM25			= 1;
	public final static int SPH_RANK_NONE			= 2;
	public final static int SPH_RANK_WORDCOUNT		= 3;

	/* sorting modes */
	public final static int SPH_SORT_RELEVANCE		= 0;
	public final static int SPH_SORT_ATTR_DESC		= 1;
	public final static int SPH_SORT_ATTR_ASC		= 2;
	public final static int SPH_SORT_TIME_SEGMENTS	= 3;
	public final static int SPH_SORT_EXTENDED		= 4;
	public final static int SPH_SORT_EXPR			= 5;

	/* grouping functions */
	public final static int SPH_GROUPBY_DAY			= 0;
	public final static int SPH_GROUPBY_WEEK		= 1;
	public final static int SPH_GROUPBY_MONTH		= 2;
	public final static int SPH_GROUPBY_YEAR		= 3;
	public final static int SPH_GROUPBY_ATTR		= 4;
	public final static int SPH_GROUPBY_ATTRPAIR	= 5;

	/* searchd reply status codes */
	public final static int SEARCHD_OK				= 0;
	public final static int SEARCHD_ERROR			= 1;
	public final static int SEARCHD_RETRY			= 2;
	public final static int SEARCHD_WARNING			= 3;

	/* attribute types */
	public final static int SPH_ATTR_INTEGER		= 1;
	public final static int SPH_ATTR_TIMESTAMP		= 2;
	public final static int SPH_ATTR_ORDINAL		= 3;
	public final static int SPH_ATTR_BOOL			= 4;
	public final static int SPH_ATTR_FLOAT			= 5;
	public final static int SPH_ATTR_MULTI			= 0x40000000;


	/* searchd commands */
	public final static int SEARCHD_COMMAND_SEARCH		= 0;
	public final static int SEARCHD_COMMAND_EXCERPT	= 1;
	public final static int SEARCHD_COMMAND_UPDATE		= 2;
	public final static int SEARCHD_COMMAND_KEYWORDS	= 3;

	/* searchd command versions */
	public final static int VER_MAJOR_PROTO		= 0x1;
	public final static int VER_COMMAND_SEARCH		= 0x113;
	public final static int VER_COMMAND_EXCERPT	= 0x100;
	public final static int VER_COMMAND_UPDATE		= 0x101;
	public final static int VER_COMMAND_KEYWORDS	= 0x100;

	/* filter types */
	private final static int SPH_FILTER_VALUES		= 0;
	private final static int SPH_FILTER_RANGE		= 1;
	// TODO FIXME private final static int SPH_FILTER_FLOATRANGE	= 2;


	private String		_host;
	private int			_port;
	private int			_offset;
	private int			_limit;
	private int			_mode;
	private int[]		_weights;
	private int			_sort;
	private String		_sortby;
	private int			_minId;
	private int			_maxId;
	private ByteArrayOutputStream	_rawFilters;
	private DataOutputStream		_filters;
	private int			_filterCount;
	private String		_groupBy;
	private int			_groupFunc;
	private String		_groupSort;
	private String		_groupDistinct;
	private int			_maxMatches;
	private int			_cutoff;
	private int			_retrycount;
	private int			_retrydelay;
	private String		_latitudeAttr;
	private String		_longitudeAttr;
	private float		_latitude;
	private float		_longitude;
	private String		_error;
	private String		_warning;
	private ArrayList	_reqs;
	private Map			_indexWeights;
	private int			_ranker;
	private int			_maxQueryTime;
	private Map			_fieldWeights;

	public static final int SPH_CLIENT_TIMEOUT_MILLISEC	= 30000;

	/** Creates a new SphinxClient instance. */
	public SphinxClient()
	{
		this("localhost", 3312);
	}

	/** Creates a new SphinxClient instance, with host:port specification. */
	public SphinxClient(String host, int port)
	{
		_host	= host;
		_port	= port;
		_offset	= 0;
		_limit	= 20;
		_mode	= SPH_MATCH_ALL;
		_sort	= SPH_SORT_RELEVANCE;
		_sortby	= "";
		_minId	= 0;
		_maxId	= 0xFFFFFFFF;

		_filterCount	= 0;
		_rawFilters		= new ByteArrayOutputStream();
		_filters		= new DataOutputStream(_rawFilters);

		_groupBy		= "";
		_groupFunc		= SPH_GROUPBY_DAY;
		_groupSort		= "@group desc";
		_groupDistinct	= "";

		_maxMatches		= 1000;
		_cutoff			= 0;
		_retrycount		= 0;
		_retrydelay		= 0;

		_latitudeAttr	= null;
		_longitudeAttr	= null;
		_latitude		= 0;
		_longitude		= 0;

		_error			= "";
		_warning		= "";

		_reqs			= new ArrayList();
		_weights		= null;
		_indexWeights	= new LinkedHashMap();
		_fieldWeights	= new LinkedHashMap();
		_ranker			= SPH_RANK_PROXIMITY_BM25;
	}

	/** Get last error message, if any. */
	public String getLastError()
	{
		return _error;
	}

	/** Get last warning message, if any. */
	public String getLastWarning()
	{
		return _warning;
	}

	/** Set searchd host and port to connect to. */
	public void setServer(String host, int port) throws SphinxException
	{
		myAssert ( host!=null && host.length()>0, "host name must not be empty" );
		myAssert ( port>0 && port<65536, "port must be in 1..65535 range" );
		_host = host;
		_port = port;
	}

	/** Internal method. Sanity check. */
	private void myAssert ( boolean condition, String err ) throws SphinxException
	{
		if ( !condition )
		{
			_error = err;
			throw new SphinxException ( err );
		}
	}

	/** Internal method. String IO helper. */
	static void writeNetUTF8(DataOutputStream ostream, String str)
			throws IOException {
		
		ostream.writeShort(0);
		if (str == null) {
			ostream.writeShort(0);
		} else {
			ostream.writeUTF(str);
		}
	}

	/** Internal method. String IO helper. */
	static String readNetUTF8(DataInputStream istream) throws IOException {
		istream.readUnsignedShort (); /* searchd emits dword lengths, but Java expects words; lets just skip first 2 bytes */
		return istream.readUTF ();
	}

	/** Internal method. Unsigned int IO helper. */
	static long readDword ( DataInputStream istream ) throws IOException {
		long v = (long) istream.readInt();
		if (v < 0) {
			v += MAX_DWORD;
		}
		return v;
	}

	/** Exchange versions with sphinx. 
	 * @throws SphinxException */
	private void hello(DataInputStream sIn, DataOutputStream sOut) throws SphinxException, IOException {
		int version = sIn.readInt();
		if (version<1){
			throw new SphinxException("expected searchd protocol version 1+, got version " + version);
		}
		sOut.writeInt(VER_MAJOR_PROTO);
	}

	void close(Closeable c) {
		if (c != null) {
			try {
				c.close();
			} catch (IOException e) {
			}
		}
	}
	
	protected Socket getSocket() throws UnknownHostException, IOException {
		Socket socket = new Socket ( _host, _port );
		socket.setSoTimeout ( SPH_CLIENT_TIMEOUT_MILLISEC );
		return socket;
	}

	/** Internal method. Get and check response packet from searchd. 
	 * @throws SphinxException */
	private byte[] response ( DataInputStream sIn ) throws SphinxException{
		
		/*  response */
		short status = 0, ver = 0;
		int len = 0;
		
		try{
			/* read status fields */
			status = sIn.readShort();
			ver = sIn.readShort();
			len = sIn.readInt();

			/* read response if non-empty */
			if (len <= 0){
				throw new SphinxException("invalid response packet size (len=" + len + ")");
			}

			byte[] response = new byte[len];
			sIn.readFully(response, 0, len);

			/* check status */
			switch (status) {
				case SEARCHD_OK:
					break;
				case SEARCHD_WARNING:
					DataInputStream in = new DataInputStream ( new ByteArrayInputStream ( response ) );
					int iWarnLen = in.readInt ();
					_warning = new String ( response, 4, iWarnLen );
					System.arraycopy ( response, 4+iWarnLen, response, 0, response.length-4-iWarnLen );
					break;
				case SEARCHD_ERROR:
					throw new SphinxException("searchd error: " + new String ( response, 4, response.length-4 ));
				case SEARCHD_RETRY:
					throw new SphinxException("temporary searchd error: " + new String ( response, 4, response.length-4 ));
				default:
					throw new SphinxException("searched returned unknown status, code=" + status);
			}
			
			return response;
			
		} catch ( IOException e ){
			String message = "received zero-sized searchd response (searchd crashed?): " + e.getMessage();
			if (len != 0) {
				/* get trace, to provide even more failure details */
				PrintWriter ew = new PrintWriter ( new StringWriter() );
				e.printStackTrace ( ew );
				ew.flush ();
				ew.close ();
				String sTrace = ew.toString ();

				/* build error message */
				message = "failed to read searchd response (status=" + status + ", ver=" + ver + ", len=" + len + ", trace=" + sTrace +")";
			}
			throw new SphinxException(message);
		}
	}
	

	/** Internal method. Connect to searchd, send request, get response as DataInputStream. 
	 * @throws SphinxException */
	DataInputStream executeCommand (int command, int version, ByteArrayOutputStream req ) throws SphinxException{
		/* connect */
		Socket sock = null; 
		InputStream sIn = null;
		OutputStream sOut = null;
	   	try {
			sock = getSocket();
			sIn = sock.getInputStream();
			sOut =  sock.getOutputStream();
			DataInputStream dIn = new DataInputStream(sIn);
			DataOutputStream dOut = new DataOutputStream(sOut);
	   		hello(dIn, dOut);
	   		request(command, version, req, dOut);
	   		byte[] data = response(dIn);
			/* spawn that tampon */
			return new DataInputStream(new ByteArrayInputStream (data));
		} catch (ConnectException e) {
			throw new SphinxException("connection to " + _host + ":" + _port + " failed: " + e);
		} catch (SphinxException e) {
			throw e;
		} catch ( Exception e ) {
			throw new SphinxException("network error: " + e);
		} finally {
			close(sIn);
			close(sOut);
			close(sock);
		}
	}

	/**
	 *  Send request to sphinx.
	 * @param command
	 * @param version
	 * @param req
	 * @param dOut
	 * @throws IOException
	 */
	private void request(int command, int version, ByteArrayOutputStream req,
			DataOutputStream dOut) throws IOException {
		dOut.writeShort (command);
		dOut.writeShort (version);
		byte[] reqBytes = req.toByteArray();
		dOut.writeInt(reqBytes.length);
		dOut.write(reqBytes);
	}

	void close(Socket sock) {
		if (sock != null) {
			try {
				sock.close();
			} catch (IOException e) {
			}
		}
	}

	/** Set matches offset and limit to return to client, max matches to retrieve on server, and cutoff. */
	public void setLimits ( int offset, int limit, int max, int cutoff ) throws SphinxException
	{
		myAssert ( offset>=0, "offset must not be negative" );
		myAssert ( limit>0, "limit must be positive" );
		myAssert ( max>0, "max must be positive" );
		myAssert ( cutoff>=0, "cutoff must not be negative" );

		_offset = offset;
		_limit = limit;
		_maxMatches = max;
		_cutoff = cutoff;
	}

	/** Set matches offset and limit to return to client, and max matches to retrieve on server. */
	public void setLimits ( int offset, int limit, int max ) throws SphinxException
	{
		setLimits ( offset, limit, max, _cutoff );
	}

	/** Set matches offset and limit to return to client. */
	public void setLimits ( int offset, int limit) throws SphinxException
	{
		setLimits ( offset, limit, _maxMatches, _cutoff );
	}

	/** Set maximum query time, in milliseconds, per-index, 0 means "do not limit". */
	public void setMaxQueryTime ( int maxTime ) throws SphinxException
	{
		myAssert ( maxTime>=0, "max_query_time must not be negative" );
		_maxQueryTime = maxTime;
	}

	/** Set matching mode. */
	public void setMatchMode(int mode) throws SphinxException
	{
		myAssert (
			mode==SPH_MATCH_ALL ||
			mode==SPH_MATCH_ANY ||
			mode==SPH_MATCH_PHRASE ||
			mode==SPH_MATCH_BOOLEAN ||
			mode==SPH_MATCH_EXTENDED ||
			mode==SPH_MATCH_EXTENDED2, "unknown mode value; use one of the SPH_MATCH_xxx constants" );
		_mode = mode;
	}

	/** Set ranking mode. */
	public void setRankingMode ( int ranker ) throws SphinxException
	{
		myAssert ( ranker==SPH_RANK_PROXIMITY_BM25
			|| ranker==SPH_RANK_BM25
			|| ranker==SPH_RANK_NONE
			|| ranker==SPH_RANK_WORDCOUNT, "unknown ranker value; use one of the SPH_RANK_xxx constants" );
		_ranker = ranker;
	}

	/** Set sorting mode. */
	public void setSortMode ( int mode, String sortby ) throws SphinxException
	{
		myAssert (
			mode==SPH_SORT_RELEVANCE ||
			mode==SPH_SORT_ATTR_DESC ||
			mode==SPH_SORT_ATTR_ASC ||
			mode==SPH_SORT_TIME_SEGMENTS ||
			mode==SPH_SORT_EXTENDED, "unknown mode value; use one of the available SPH_SORT_xxx constants" );
		myAssert ( mode==SPH_SORT_RELEVANCE || ( sortby!=null && sortby.length()>0 ), "sortby string must not be empty in selected mode" );

		_sort = mode;
		_sortby = ( sortby==null ) ? "" : sortby;
	}

	/** Set per-field weights (all values must be positive). WARNING: DEPRECATED, use SetFieldWeights() instead. */
	public void setWeights(int[] weights) throws SphinxException {
		myAssert ( weights!=null, "weights must not be null" );
		for (int i = 0; i < weights.length; i++) {
			int weight = weights[i];
			myAssert ( weight>0, "all weights must be greater than 0" );
		}
		_weights = new int[weights.length];
		System.arraycopy(weights, 0, _weights, 0, weights.length);
	}

	/**
	 * Get per-field weights.
	 * @return array of values.
	 */
	public int[] getWeights() {
		int[] weights = new int[_weights.length];
		System.arraycopy(_weights, 0, weights, 0, _weights.length);
		return weights;
	}
	
	/**
	 * Bind per-field weights by field name.
	 * @param fieldWeights hash which maps String index names to Integer weights
	 */
	public void setFieldeights ( Map fieldWeights ) throws SphinxException
	{
		/* FIXME! implement checks here */
		_fieldWeights = ( fieldWeights==null ) ? new LinkedHashMap () : fieldWeights;
	}

	/**
	 * Bind per-index weights by index name (and enable summing the weights on duplicate matches, instead of replacing them).
	 * @param indexWeights hash which maps String index names to Integer weights
	 */
	public void setIndexWeights ( Map indexWeights ) throws SphinxException
	{
		/* FIXME! implement checks here */
		_indexWeights = ( indexWeights==null ) ? new LinkedHashMap () : indexWeights;
	}

	/** Set document IDs range to match. */
	public void setIDRange ( int min, int max ) throws SphinxException
	{
		myAssert ( min<=max, "min must be less or equal to max" );
		_minId = min;
		_maxId = max;
	}

	/** Set values filter. Only match records where attribute value is in given set. */
	public void setFilter ( String attribute, int[] values, boolean exclude ) throws SphinxException
	{
		myAssert ( values!=null && values.length>0, "values array must not be null or empty" );
		myAssert ( attribute!=null && attribute.length()>0, "attribute name must not be null or empty" );

		try
		{
			writeNetUTF8 ( _filters, attribute );
			_filters.writeInt ( SPH_FILTER_VALUES );
			_filters.writeInt ( values.length );
			for ( int i=0; i<values.length; i++ )
				_filters.writeInt ( values[i] );
			_filters.writeInt ( exclude ? 1 : 0 );

		} catch ( Exception e )
		{
			myAssert ( false, "IOException: " + e.getMessage() );
		}
		_filterCount++;
	}

	/** Set values filter with a single value (syntax sugar; see {@link #setFilter(String,int[],boolean)}). */
	public void setFilter ( String attribute, int value, boolean exclude ) throws SphinxException
	{
		int[] values = new int[] { value };
		setFilter ( attribute, values, exclude );
	}

	/** Set integer range filter.  Only match records if attribute value is beetwen min and max (inclusive). */
	public void setFilterRange ( String attribute, int min, int max, boolean exclude ) throws SphinxException
	{
		myAssert ( min<=max, "min must be less or equal to max" );
		try
		{
			writeNetUTF8 ( _filters, attribute );
			_filters.writeInt ( SPH_FILTER_RANGE );
			_filters.writeInt ( min );
			_filters.writeInt ( max );
			_filters.writeInt ( exclude ? 1 : 0 );

		} catch ( Exception e ) {
			myAssert ( false, "IOException: " + e.getMessage());
		}
		_filterCount++;
	}

	/** Set float range filter.  Only match records if attribute value is beetwen min and max (inclusive). */
	public void setFilterFloatRange ( String attribute, float min, float max, boolean exclude ) 
		throws SphinxException {
		
		myAssert ( min<=max, "min must be less or equal to max" );
		try {
			writeNetUTF8(_filters, attribute);
			_filters.writeInt(SPH_FILTER_RANGE);
			_filters.writeFloat(min);
			_filters.writeFloat(max);
			_filters.writeInt(exclude ? 1 : 0);
		} catch (Exception e) {
			myAssert ( false, "IOException: " + e.getMessage() );
		}
		_filterCount++;
	}

	/** Setup geographical anchor point. Required to use @geodist in filters and sorting; distance will be computed to this point. */
	public void setGeoAnchor(String latitudeAttr, String longitudeAttr, float latitude, float longitude) 
		throws SphinxException {
		myAssert ( latitudeAttr!=null && latitudeAttr.length()>0, "longitudeAttr string must not be null or empty" );
		myAssert ( longitudeAttr!=null && longitudeAttr.length()>0, "longitudeAttr string must not be null or empty" );

		_latitudeAttr = latitudeAttr;
		_longitudeAttr = longitudeAttr;
		_latitude = latitude;
		_longitude = longitude;
	}

	/** Set grouping attribute and function. */
	public void setGroupBy(String attribute, int func, String groupsort) throws SphinxException {
		myAssert (
			func==SPH_GROUPBY_DAY ||
			func==SPH_GROUPBY_WEEK ||
			func==SPH_GROUPBY_MONTH ||
			func==SPH_GROUPBY_YEAR ||
			func==SPH_GROUPBY_ATTR ||
			func==SPH_GROUPBY_ATTRPAIR, "unknown func value; use one of the available SPH_GROUPBY_xxx constants" );

		_groupBy = attribute;
		_groupFunc = func;
		_groupSort = groupsort;
	}

	/** Set grouping attribute and function with default ("@group desc") groupsort (syntax sugar). */
	public void setGroupBy(String attribute, int func) throws SphinxException {
		setGroupBy(attribute, func, "@group desc");
	}

	/** Set count-distinct attribute for group-by queries. */
	public void setGroupDistinct(String attribute) {
		_groupDistinct = attribute;
	}

	/** Set distributed retries count and delay. */
	public void setRetries(int count, int delay) throws SphinxException {
		myAssert ( count>=0, "count must not be negative" );
		myAssert ( delay>=0, "delay must not be negative" );
		_retrycount = count;
		_retrydelay = delay;
	}

	/** Set distributed retries count with default (zero) delay (syntax sugar). */
	public void setRetries(int count) throws SphinxException {
		setRetries (count, 0);
	}

	/** Reset all currently set filters (for multi-queries). */
	public void resetFilters()
	{
		/* should we close them first? */
		_rawFilters = new ByteArrayOutputStream();
		_filters = new DataOutputStream(_rawFilters);
		_filterCount = 0;

		/* reset GEO anchor */
		_latitudeAttr = null;
		_longitudeAttr = null;
		_latitude = 0;
		_longitude = 0;
	}

	/** Connect to searchd server and run current search query against all indexes (syntax sugar). */
	public SphinxResult query ( String query ) throws SphinxException
	{
		return query ( query, "*", "" );
	}

	/** Connect to searchd server and run current search query against all indexes (syntax sugar). */
	public SphinxResult query ( String query, String index ) throws SphinxException
	{
		return query ( query, index, "" );
	}

	/** Connect to searchd server and run current search query. */
	public SphinxResult query ( String query, String index, String comment ) throws SphinxException
	{
		myAssert ( _reqs==null || _reqs.size()==0, "AddQuery() and Query() can not be combined; use RunQueries() instead" );

		addQuery ( query, index, comment );
		SphinxResult[] results = runQueries();
		SphinxResult res = results[0];
		_warning = res.warning;
		_error = res.error;
		return res;
	}

	/** Add new query with current settings to current search request. */
	public int addQuery ( String query, String index, String comment ) throws SphinxException
	{
		ByteArrayOutputStream req = new ByteArrayOutputStream();

		/* build request */
		try {
			DataOutputStream out = new DataOutputStream(req);
			out.writeInt(_offset);
			out.writeInt(_limit);
			out.writeInt(_mode);
			out.writeInt(_ranker);
			out.writeInt(_sort);
			writeNetUTF8(out, _sortby);
			writeNetUTF8(out, query);
			int weightLen = _weights != null ? _weights.length : 0;

			out.writeInt(weightLen);
			if (_weights != null) {
				for (int i = 0; i < _weights.length; i++)
					out.writeInt(_weights[i]);
			}

			writeNetUTF8(out, index);
			out.writeInt(0);
			out.writeInt(_minId);
			out.writeInt(_maxId);

			/* filters */
			out.writeInt(_filterCount);
			out.write(_rawFilters.toByteArray());

			/* group-by, max matches, sort-by-group flag */
			out.writeInt(_groupFunc);
			writeNetUTF8(out, _groupBy);
			out.writeInt(_maxMatches);
			writeNetUTF8(out, _groupSort);

			out.writeInt(_cutoff);
			out.writeInt(_retrycount);
			out.writeInt(_retrydelay);

			writeNetUTF8(out, _groupDistinct);

			/* anchor point */
			if (_latitudeAttr == null || _latitudeAttr.length() == 0 || _longitudeAttr == null || _longitudeAttr.length() == 0) {
				out.writeInt(0);
			} else {
				out.writeInt(1);
				writeNetUTF8(out, _latitudeAttr);
				writeNetUTF8(out, _longitudeAttr);
				out.writeFloat(_latitude);
				out.writeFloat(_longitude);

			}

			/* per-index weights */
			out.writeInt(_indexWeights.size());
			for (Iterator e = _indexWeights.keySet().iterator(); e.hasNext();) {
				String indexName = (String) e.next();
				Integer weight = (Integer) _indexWeights.get(indexName);
				writeNetUTF8(out, indexName);
				out.writeInt(weight.intValue());
			}

			/* max query time */
			out.writeInt ( _maxQueryTime );

			/* per-field weights */
			out.writeInt ( _fieldWeights.size() );
			for ( Iterator e=_fieldWeights.keySet().iterator(); e.hasNext(); )
			{
				String field = (String) e.next();
				Integer weight = (Integer) _fieldWeights.get ( field );
				writeNetUTF8 ( out, field );
				out.writeInt ( weight.intValue() );
			}

			/* comment */
			writeNetUTF8 ( out, comment );

			/* done! */
			out.flush ();
			int qIndex = _reqs.size();
			_reqs.add ( qIndex, req.toByteArray() );
			return qIndex;

		} catch (IOException e){
			myAssert ( false, "error in AddQuery(): " + e + ": " + e.getMessage() );
		} finally {
			try {
				_filters.close ();
				_rawFilters.close ();
			} catch(IOException e) {
				myAssert ( false, "error in AddQuery(): " + e + ": " + e.getMessage() );
			}
		}
		return -1;
	}

	/** Run all previously added search queries. */
	public SphinxResult[] runQueries() throws SphinxException {
		
		myAssert( _reqs!=null && !_reqs.isEmpty(), "no queries defined, issue AddQuery() first");

		/* build the mega-request */
		int nreqs = _reqs.size();
		ByteArrayOutputStream reqBuf = new ByteArrayOutputStream();
		try {
			DataOutputStream req = new DataOutputStream ( reqBuf );
			req.writeInt ( nreqs );
			for ( int i=0; i<nreqs; i++ ) {
				req.write ( (byte[]) _reqs.get(i) );
			}
			req.flush ();

		} catch ( Exception e ) {
			throw new SphinxException ("internal error: failed to build request: " + e);
		}

		DataInputStream in = executeCommand ( SEARCHD_COMMAND_SEARCH, VER_COMMAND_SEARCH, reqBuf );
		SphinxResult[] results = new SphinxResult [ nreqs ];
		_reqs = new ArrayList();

		try {
			for ( int ires=0; ires<nreqs; ires++ ) {
				SphinxResult res = new SphinxResult();
				results[ires] = res;

				int status = in.readInt();
				res.setStatus ( status );
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
				for (int i = 0; i < nfields; i++)
					res.fields[i] = readNetUTF8(in);

				/* read arrts */
				int nattrs = in.readInt();
				res.attrTypes = new int[nattrs];
				res.attrNames = new String[nattrs];
				for (int i = 0; i < nattrs; i++) {
					String AttrName = readNetUTF8(in);
					int AttrType = in.readInt();
					res.attrNames[i] = AttrName;
					res.attrTypes[i] = AttrType;
				}

				/* read match count */
				int count = in.readInt();
				int id64 = in.readInt();
				res.matches = new SphinxMatch[count];
				for ( int matchesNo=0; matchesNo<count; matchesNo++ )
				{
					SphinxMatch docInfo;
					docInfo = new SphinxMatch (
							( id64==0 ) ? readDword(in) : in.readLong(),
							in.readInt() );

					/* read matches */
					for (int attrNumber = 0; attrNumber < res.attrTypes.length; attrNumber++)
					{
						//TODO FIXME: String attrName = res.attrNames[attrNumber];
						int type = res.attrTypes[attrNumber];

						/* handle floats */
						if ( type==SPH_ATTR_FLOAT )
						{
							docInfo.attrValues.add ( attrNumber, new Float ( in.readFloat() ) );
							continue;
						}

						/* handle everything else as unsigned ints */
						long val = readDword ( in );
						if ( ( type & SPH_ATTR_MULTI )!=0 )
						{
							long[] vals = new long [ (int)val ];
							for ( int k=0; k<val; k++ )
								vals[k] = readDword ( in );

							docInfo.attrValues.add ( attrNumber, vals );

						} else
						{
							docInfo.attrValues.add ( attrNumber, new Long ( val ) );
						}
					}
					res.matches[matchesNo] = docInfo;
				}

				res.total = in.readInt();
				res.totalFound = in.readInt();
				res.time = in.readInt() / 1000.0f;

				res.words = new SphinxWordInfo [ in.readInt() ];
				for ( int i=0; i<res.words.length; i++ )
					res.words[i] = new SphinxWordInfo ( readNetUTF8(in), readDword(in), readDword(in) );
			}
			return results;

		} catch ( IOException e ) {
			throw new SphinxException("incomplete reply");
		}
	}

	/**
	 * Connect to searchd server and generate excerpts (snippets) from given documents.
	 * @param opts maps String keys to String or Integer values (see the documentation for complete keys list).
	 * @return null on failure, array of snippets on success.
	 */
	public String[] buildExcerpts ( String[] docs, String index, String words, Map opts ) throws SphinxException
	{
		myAssert(docs != null && docs.length > 0, "BuildExcerpts: Have no documents to process");
		myAssert(index != null && index.length() > 0, "BuildExcerpts: Have no index to process documents");
		myAssert(words != null && words.length() > 0, "BuildExcerpts: Have no words to highlight");
		if (opts == null) opts = new LinkedHashMap();

		/* fixup options */
		if (!opts.containsKey("before_match")) opts.put("before_match", "<b>");
		if (!opts.containsKey("after_match")) opts.put("after_match", "</b>");
		if (!opts.containsKey("chunk_separator")) opts.put("chunk_separator", "...");
		if (!opts.containsKey("limit")) opts.put("limit", new Integer(256));
		if (!opts.containsKey("around")) opts.put("around", new Integer(5));
		if (!opts.containsKey("exact_phrase")) opts.put("exact_phrase", new Integer(0));
		if (!opts.containsKey("single_passage")) opts.put("single_passage", new Integer(0));
		if (!opts.containsKey("use_boundaries")) opts.put("use_boundaries", new Integer(0));
		if (!opts.containsKey("weight_order")) opts.put("weight_order", new Integer(0));

		/* build request */
		ByteArrayOutputStream reqBuf = new ByteArrayOutputStream();
		DataOutputStream req = new DataOutputStream ( reqBuf );
		try{
			req.writeInt(0);
			int iFlags = 1; /* remove_spaces */
			if ( ((Integer)opts.get("exact_phrase")).intValue()!=0 )	iFlags |= 2;
			if ( ((Integer)opts.get("single_passage")).intValue()!=0 )	iFlags |= 4;
			if ( ((Integer)opts.get("use_boundaries")).intValue()!=0 )	iFlags |= 8;
			if ( ((Integer)opts.get("weight_order")).intValue()!=0 )	iFlags |= 16;
			req.writeInt ( iFlags );
			writeNetUTF8 ( req, index );
			writeNetUTF8 ( req, words );

			/* send options */
			writeNetUTF8 ( req, (String) opts.get("before_match") );
			writeNetUTF8 ( req, (String) opts.get("after_match") );
			writeNetUTF8 ( req, (String) opts.get("chunk_separator") );
			req.writeInt ( ((Integer) opts.get("limit")).intValue() );
			req.writeInt ( ((Integer) opts.get("around")).intValue() );

			/* send documents */
			req.writeInt ( docs.length );
			for ( int i=0; i<docs.length; i++ )
				writeNetUTF8 ( req, docs[i] );

			req.flush();

		} catch ( Exception e ) {
			throw new SphinxException("internal error: failed to build request: " + e);
		}

		DataInputStream in = executeCommand ( SEARCHD_COMMAND_EXCERPT, VER_COMMAND_EXCERPT, reqBuf );
		try {
			String[] res = new String [ docs.length ];
			for ( int i=0; i<docs.length; i++ ){
				res[i] = readNetUTF8 ( in );
			}
			return res;

		} catch(IOException e) {
			throw new SphinxException("incomplete reply");
		}
	}

	/**
	 * Connect to searchd server and update given attributes on given documents in given indexes.
	 * Sample code that will set group_id=123 where id=1 and group_id=456 where id=3:
	 *
	 * <pre>
	 * String[] attrs = new String[1];
	 *
	 * attrs[0] = "group_id";
	 * long[][] values = new long[2][2];
	 *
	 * values[0] = new long[2]; values[0][0] = 1; values[0][1] = 123;
	 * values[1] = new long[2]; values[1][0] = 3; values[1][1] = 456;
	 *
	 * int res = cl.UpdateAttributes ( "test1", attrs, values );
	 * </pre>
	 *
	 * @param index		index name(s) to update; might be distributed
	 * @param attrs		array with the names of the attributes to update
	 * @param values	array of updates; each long[] entry must contains document ID
	 *					in the first element, and all new attribute values in the following ones
	 * @return			-1 on failure, amount of actually found and updated documents (might be 0) on success
	 *
	 * @throws			SphinxException on invalid parameters
	 */
	public int updateAttributes ( String index, String[] attrs, long[][] values ) throws SphinxException
	{
		/* check args */
		myAssert ( index!=null && index.length()>0, "no index name provided" );
		myAssert ( attrs!=null && attrs.length>0, "no attribute names provided" );
		myAssert ( values!=null && values.length>0, "no update entries provided" );
		for ( int i=0; i<values.length; i++ )
		{
			myAssert ( values[i]!=null, "update entry #" + i + " is null" );
			myAssert ( values[i].length==1+attrs.length, "update entry #" + i + " has wrong length" );
		}

		/* build and send request */
		ByteArrayOutputStream reqBuf = new ByteArrayOutputStream();
		DataOutputStream req = new DataOutputStream ( reqBuf );
		
		try{
			writeNetUTF8 ( req, index );

			req.writeInt ( attrs.length );
			for ( int i=0; i<attrs.length; i++ )
				writeNetUTF8 ( req, attrs[i] );

			req.writeInt ( values.length );
			for ( int i=0; i<values.length; i++ )
			{
				req.writeLong ( values[i][0] ); /* send docid as 64bit value */
				for ( int j=1; j<values[i].length; j++ )
					req.writeInt ( (int)values[i][j] ); /* send values as 32bit values; FIXME! what happens when they are over 2^31? */
			}

			req.flush();

		} catch ( Exception e ) {
			throw new SphinxException("internal error: failed to build request: " + e);
		}

		/* get and parse response */
		DataInputStream in = executeCommand ( SEARCHD_COMMAND_UPDATE, VER_COMMAND_UPDATE, reqBuf );

		try {
			return in.readInt ();
		} catch ( Exception e ) {
			throw new SphinxException("incomplete reply");
		}
	}

	/**
     * Connect to searchd server, and generate keyword list for a given query.
     * Returns null on failure, an array of Maps with misc per-keyword info on success.
     */
	public Map[] buildKeywords ( String query, String index, boolean hits ) throws SphinxException
	{
		/* build request */
		ByteArrayOutputStream reqBuf = new ByteArrayOutputStream();
		DataOutputStream req = new DataOutputStream ( reqBuf );
		try {
			writeNetUTF8 ( req, query );
			writeNetUTF8 ( req, index );
			req.writeInt ( hits ? 1 : 0 );

		} catch (IOException e) {
			throw new SphinxException("internal error: failed to build request: " + e);
		}

		/* run request */
		DataInputStream in = executeCommand ( SEARCHD_COMMAND_KEYWORDS, VER_COMMAND_KEYWORDS, reqBuf );
		/* parse reply */
		try {
			int iNumWords = in.readInt ();
			Map[] res = new Map[iNumWords];

			for ( int i=0; i<iNumWords; i++ )
			{
				res[i] = new LinkedHashMap();
				res[i].put ( "tokenized", readNetUTF8 ( in ) );
				res[i].put ( "normalized", readNetUTF8 ( in ) );
				if (hits) {
					res[i].put ( "docs", Long.valueOf(readDword ( in )) );
					res[i].put ( "hits", Long.valueOf(readDword ( in )) );
				}
			}
			return res;

		} catch ( IOException e ) {
			throw new SphinxException("incomplete reply");
		}
	}
}
