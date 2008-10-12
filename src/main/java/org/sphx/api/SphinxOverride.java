package org.sphx.api;

import java.util.Map;

/**
 * Sphinx Override inner class.
 * 
 */
public class SphinxOverride {
	private String attrName;
	private int attrType;
	private Map values;

	/**
	 * Get attribute name.
	 * 
	 * @return the attribute name
	 */
	public String getAttrName() {
		return attrName;
	}

	/**
	 * Set attribute name.
	 * 
	 * @param name
	 *            attribute name
	 */
	public void setAttrName(final String name) {
		this.attrName = name;
	}

	/**
	 * Get attribute type.
	 * 
	 * @return the attribute type
	 */
	public int getAttrType() {
		return attrType;
	}

	/**
	 * Set attribute type.
	 * 
	 * @param type
	 *            attribute type
	 */
	public void setAttrType(final int type) {
		this.attrType = type;
	}

	/**
	 * Get map document IDs to attribute values.
	 * 
	 * @return the values
	 */
	public Map getValues() {
		return values;
	}

	/**
	 * Set map document IDs to attribute values.
	 * 
	 * @param map
	 *            values
	 */
	public void setValues(final Map map) {
		this.values = map;
	}

	/**
	 * Get hash code.
	 * @return hashcode
	 */
	public int hashCode() {
		int result = 0;
		if (attrName != null) {
			result = attrName.hashCode();
		}
		return result;
	}

	/**
	 * Equals.
	 * @param obj another object
	 * @return true if equal
	 */
	public boolean equals(final Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		final SphinxOverride other = (SphinxOverride) obj;
		if (attrName == null) {
			if (other.attrName != null) {
				return false;
			}
		} else if (!attrName.equals(other.attrName)) {
			return false;
		}
		return true;
	}

}
