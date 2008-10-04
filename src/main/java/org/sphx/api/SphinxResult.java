package org.sphx.api;

/**
 * Search result set.
 * Includes retrieved matches array, status code and error/warning messages,
 * query stats, and per-word stats.
 */
public class SphinxResult {
	/** Full-text field namess. */
	public String[] fields;

	/** Attribute names. */
	public String[] attrNames;

	/** Attribute types (refer to SPH_ATTR_xxx constants
	 *in SphinxClient). */
	public int[] attrTypes;

	/** Retrieved matches. */
	public SphinxMatch[] matches;

	/** Total matches in this result set. */
	public int total;

	/** Total matches found in the index(es). */
	public int totalFound;

	/** Elapsed time (as reported by searchd), in seconds. */
	public float time;

	/** Per-word statistics. */
	public SphinxWordInfo[] words;

	/** Warning message, if any. */
	public String warning;

	/** Error message, if any. */
	public String error;

	/** Query status (refer to SEARCHD_xxx constants in SphinxClient). */
	private int status = -1;

	/** Trivial constructor, initializes an empty result set. */
	public SphinxResult() {
		this.attrNames = new String[0];
		this.matches = new SphinxMatch[0];
		this.words = new SphinxWordInfo[0];
		this.fields = new String[0];
		this.attrTypes = new int[0];
	}

	/** Get query status.
	 * @return status.
	 */
	public final int getStatus() {
		return status;
	}

	/** Set query status (accessible from API package only).
	 * @param status status for result.
	 */
	final void setStatus(final int status) {
		this.status = status;
	}
}
