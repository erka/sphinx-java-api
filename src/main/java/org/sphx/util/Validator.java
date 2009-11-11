package org.sphx.util;

import java.util.Collection;

/**
 * Help class to validate some parameters.
 * 
 * @author erka
 * 
 */
public final class Validator {

	/**
	 * It's utils class.
	 */
	private Validator() {
		super();
	}

	/**
	 * Validate that an array of strings is not empty.
	 * 
	 * @param elements
	 *            an array of stings
	 * @return true if not empty
	 */
	public static boolean isNotEmpty(final String[] elements) {
		return elements != null && elements.length != 0;
	}

	/**
	 * Validate that a string is not empty.
	 * 
	 * @param string
	 *            a string
	 * @return true if not empty
	 */
	public static boolean isNotEmpty(final String string) {
		return string != null && string.length() != 0;
	}

	/**
	 * Validate that an array of integers is not empty.
	 * 
	 * @param elements
	 *            an array of integers
	 * @return true if not empty
	 */
	public static boolean isNotEmpty(final int[] elements) {
		return elements != null && elements.length != 0;
	}

	/**
	 * Validate that a collection is not empty.
	 * 
	 * @param collection
	 *            a collection
	 * @return true if not empty
	 */
	public static boolean isNotEmpty(final Collection<?> collection) {
		return collection != null && !collection.isEmpty();
	}

	/**
	 * Validate that a collection is empty.
	 * 
	 * @param collection
	 *            a collection
	 * @return true if not empty
	 */
	public static boolean isEmpty(final Collection<?> collection) {
		return collection == null || collection.isEmpty();
	}

}
