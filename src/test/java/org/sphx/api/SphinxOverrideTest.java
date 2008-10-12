package org.sphx.api;

import junit.framework.TestCase;

public class SphinxOverrideTest extends TestCase {

	private SphinxOverride sphinxOverride = new SphinxOverride();
	
	public void testHashCode() {
		assertEquals(0, sphinxOverride.hashCode());
		sphinxOverride.setAttrName("name");
		assertEquals("name".hashCode(), sphinxOverride.hashCode());
	}

	public void testEquals() {
		assertTrue(sphinxOverride.equals(sphinxOverride));
		assertFalse(sphinxOverride.equals(null));
		assertFalse(sphinxOverride.equals(new Object()));
		SphinxOverride other = new SphinxOverride();
		assertTrue(sphinxOverride.equals(other));
		other.setAttrName("name1");
		assertFalse(sphinxOverride.equals(other));
		sphinxOverride.setAttrName("name");
		assertFalse(sphinxOverride.equals(other));
		sphinxOverride.setAttrName("name1");
		assertTrue(sphinxOverride.equals(other));
	}

}
