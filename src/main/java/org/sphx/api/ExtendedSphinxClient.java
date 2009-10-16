package org.sphx.api;

import java.util.List;

/**
 * Helper methods for running MATCH_EXTENDED2 queries.
 *
 * @author Michael Guymon
 */
public class ExtendedSphinxClient extends SphinxClient {
  
  /**
	 * Creates a new SphinxClient instance with a default mode of MATCH_EXTENDED2.
	 */
	public ExtendedSphinxClient() {
		this("localhost", DEFAULT_SEARCHD_PORT);
	}

  /**
   * Creates a new SphinxClient instance with a default mode of MATCH_EXTENDED2.
   *
   * @param sphinxHost String
   * @param sphinxPort int
   */
  public ExtendedSphinxClient(final String sphinxHost, final int sphinxPort) {
    super(sphinxHost, sphinxPort);
    mode = SPH_MATCH_EXTENDED2;
  }

  /**
   * Search a specific field by query.
   *
   * @param field String
   * @param query String
   * @param index String
   * @param comment String
   * @return int
   * @throws SphinxException if params are invalid
   */
  public int addFieldQuery(final String field, final String query,
                           final String index, final String comment) throws SphinxException {
    String search = new StringBuffer()
      .append("@")
      .append(field)
      .append(" \"")
      .append(query)
      .append("\"")
      .toString();

    return this.addQuery(search, index, comment);
  }

  /**
   * Search a specific {@link List} of fields by query.
   *
   * @param fields List
   * @param query String
   * @param index String
   * @param comment String
   * @return int
   * @throws SphinxException if params are invalid
   */
  public int addFieldsQuery(final List fields, final String query,
                            final String index, final String comment) throws SphinxException {
    StringBuffer search = new StringBuffer("@(");

    if (fields != null && fields.size() > 0) {
      for (Object field : fields) {
          search.append(field.toString());
          search.append(",");
      }

      // remove trailing comma
      search.replace(search.length() - 1, search.length(), "");
    }
            
    search.append(") \"")
      .append(query)
      .append("\"");

    return addQuery(search.toString(), index, comment);
  }

}
