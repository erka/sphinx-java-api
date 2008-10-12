package org.sphx.api;

import junit.framework.TestCase;

public class SphinxExceptionTest extends TestCase {

	public void testSphinxException() {
		assertNull(new SphinxException().getMessage());
		String string = "Some error happend.";
		assertEquals(string, new SphinxException(string).getMessage());
	}

}
