package org.sphx.api;

import java.util.ArrayList;

/**
 * Matched document information, as in search result.
 */
public class SphinxMatch {
	/** Matched document ID. */
	public long docId;

	/** Matched document weight. */
	public int weight;

	/** Matched document attribute values. */
	public ArrayList attrValues;

	/** Trivial constructor.
	 * @param docId id of document.
	 * @param weight weight. 
	 */
	public SphinxMatch(final long docId, final int weight) {
		this.docId = docId;
		this.weight = weight;
		this.attrValues = new ArrayList();
	}
}
