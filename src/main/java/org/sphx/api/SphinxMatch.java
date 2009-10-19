package org.sphx.api;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Matched document information, as in search result.
 */
public class SphinxMatch {
	/** Matched document ID. */
	private long docId;

	/** Matched document weight. */
	private int weight;

	/** Matched document attributes. */
	private Map attributes;

	/** Trivial constructor.
	 * @param curDocId id of document.
	 * @param curWeight weight.
	 */
	public SphinxMatch(final long curDocId, final int curWeight) {
		this.docId = curDocId;
		this.weight = curWeight;
		this.attributes = new LinkedHashMap();
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
   * Set attribute.
   *
   * @param field String
   * @param value Object
   */
	public void setAttribute(final String field, final Object value) {
    this.attributes.put(field, value);
  }

  /**
   * Get attribute.
   *
   * @param field String
   * @return Object
   */
  public Object getAttribute(final String field) {
    return this.attributes.get(field);
  }

  /**
   * Get attribute.
   *
   * @param fieldPos Integer
   * @return Object
   */
  public Object getAttribute(final Integer fieldPos) {
    return this.attributes.get(this.attributes.keySet().toArray()[fieldPos]);
  }
}
