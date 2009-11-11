package org.sphx.util;

import java.util.LinkedList;

import junit.framework.TestCase;

public class ValidatorTest extends TestCase {

	public void testIsNotEmptyStringArray() {
		String[] array = null;
		assertFalse(Validator.isNotEmpty(array));
		array = new String[0];
		assertFalse(Validator.isNotEmpty(array));
		array = new String[] { "test" };
		assertTrue(Validator.isNotEmpty(array));
	}

	public void testIsNotEmptyString() {
		String string = null;
		assertFalse(Validator.isNotEmpty(string));
		string = "";
		assertFalse(Validator.isNotEmpty(string));
		string = "test";
		assertTrue(Validator.isNotEmpty(string));
	}

	public void testIsNotEmptyIntArray() {
		int[] array = null;
		assertFalse(Validator.isNotEmpty(array));
		array = new int[0];
		assertFalse(Validator.isNotEmpty(array));
		array = new int[] { 1 };
		assertTrue(Validator.isNotEmpty(array));
	}

	public void testIsNotEmptyCollectionArray() {
		LinkedList<Object> collection = null;
		assertFalse(Validator.isNotEmpty(collection));
		collection = new LinkedList<Object>();
		assertFalse(Validator.isNotEmpty(collection));
		collection.add(new Object());
		assertTrue(Validator.isNotEmpty(collection));
	}

	public void testIsEmptyCollectionArray() {
		LinkedList<Object> collection = null;
		assertTrue(Validator.isEmpty(collection));
		collection = new LinkedList<Object>();
		assertTrue(Validator.isEmpty(collection));
		collection.add(new Object());
		assertFalse(Validator.isEmpty(collection));
	}

}
