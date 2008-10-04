package org.sphx.api;

/** Per-word statistics class. */
public class SphinxWordInfo {
	/**
	 * Word form as returned from search daemon, stemmed or otherwise
	 * postprocessed.
	 */
	public String word;

	/** Total amount of matching documents in collection. */
	public long docs;

	/** Total amount of hits (occurences) in collection. */
	public long hits;

	/** Trivial constructor.
	 * @param word the word.
	 * @param docs docs found.
	 * @param hits  hits found.
	 */
	public SphinxWordInfo(final String word, final long docs,
						  final long hits) {
		this.word = word;
		this.docs = docs;
		this.hits = hits;
	}
}
