package org.sphx.api;

/** Per-word statistics class. */
public class SphinxWordInfo {
	/**
	 * Word form as returned from search daemon, stemmed or otherwise
	 * postprocessed.
	 */
	private String word;

	/** Total amount of matching documents in collection. */
	private long docs;

	/** Total amount of hits (occurences) in collection. */
	private long hits;

	/** Trivial constructor.
	 * @param currentWord the word.
	 * @param currentDocs docs found.
	 * @param currentHits  hits found.
	 */
	public SphinxWordInfo(final String currentWord,
			final long currentDocs, final long currentHits) {
		this.word = currentWord;
		this.docs = currentDocs;
		this.hits = currentHits;
	}

	/**
	 * Get Word form as returned from search daemon, stemmed or
	 *  otherwise postprocessed.
	 * @return the word
	 */
	public final String getWord() {
		return word;
	}

	/**
	 * Get total amount of matching documents in collection.
	 * @return the docs.
	 */
	public final long getDocs() {
		return docs;
	}

	/**
	 * Get total amount of hits (occurences) in collection.
	 * @return the hits
	 */
	public final long getHits() {
		return hits;
	}
}
