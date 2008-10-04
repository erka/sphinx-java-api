package org.sphx.api;

import java.util.ArrayList;

/**
 * Matched document information, as in search result.
 */
public class SphinxMatch {
	/** Matched document ID. */
	private long docId;

	/** Matched document weight. */
	private int weight;

	/** Matched document attribute values. */
	private ArrayList attrValues;

	/** Trivial constructor.
	 * @param curDocId id of document.
	 * @param curWeight weight.
	 */
	public SphinxMatch(final long curDocId, final int curWeight) {
		this.docId = curDocId;
		this.weight = curWeight;
		this.attrValues = new ArrayList();
	}

	/**
	 * Matched document ID.
	 * @return the docId
	 */
	public final long getDocId() {
		return docId;
	}

	/**
	 * Matched document weight.
	 * @return the weight
	 */
	public final int getWeight() {
		return weight;
	}

	/**
	 * Matched document attribute values.
	 * @return the attrValues
	 */
	public final ArrayList getAttrValues() {
		return attrValues;
	}

	/**
	 * Add attribute to collection.
	 * @param idx attribute index.
	 * @param obj attribute value.
	 */
	final void addAttribute(final int idx, final Object obj) {
		attrValues.add(idx, obj);
	}
}
