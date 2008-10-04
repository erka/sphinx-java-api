package org.sphx.api;

/** Exception thrown on attempts to pass invalid
 *  arguments to Sphinx API methods. */
public class SphinxException extends Exception {
	/**
	 * serial Version UID.
	 */
	private static final long serialVersionUID = 1L;

	/** Trivial constructor. */
	public SphinxException() {
	}

	/** Constructor from error message string.
	 * @param message message about error.
	 */
	public SphinxException(final String message) {
		super(message);
	}
}
