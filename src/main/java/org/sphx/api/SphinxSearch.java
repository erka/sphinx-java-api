package org.sphx.api;

import java.util.ArrayList;
import java.util.List;

/**
 * A complex Sphinx search.
 *
 * @author Michael Guymon
 */
public class SphinxSearch {

  private String index;
  private String comment;
  private List<String> queries;

  /**
   * Create new SphinxSearch.
   */
  public SphinxSearch() {
    this("*", "");
  }

  /**
   * Create new SphinxSearch.
   *
   * @param searchIndex String
   */
  public SphinxSearch(final String searchIndex) {
    this(searchIndex, "");
  }

  /**
   * Create new SphinxSearch.
   *  
   * @param searchIndex String
   * @param searchComment String
   */
  public SphinxSearch(final String searchIndex, final String searchComment) {
    this.index = searchIndex;
    this.comment = searchComment;
    this.queries = new ArrayList<String>();
  }

  /**
   * Add Query.
   *
   * @param query String
   */
  public void addQuery(final String query) {    
    queries.add(query);
  }

  /**
   * Add a query by specific field.
   *
   * @param field String
   * @param query String
   */
  public void addFieldQuery(final String field, final String query) {
    String search = new StringBuffer()
      .append("@")
      .append(field)
      .append(" \"")
      .append(query)
      .append("\"")
      .toString();

    queries.add(search);
  }

  /**
   * Add a query by specific {@link List} of fields.
   *
   * @param fields List
   * @param query String
   */
  public void addFieldsQuery(final List fields, final String query) {
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

    queries.add(search.toString());
  }

  /**
   * Get index used for this search.
   *
   * @return String
   */
  public String getIndex() {
    return index;
  }

  /**
   * Get comment used for this search. If not set, will be an empty String.
   *
   * @return String
   */
  public String getComment() {
    return comment;
  }

  /**
   * Build query for Sphinx search.
   *
   * @return String
   */
  public String buildQuery() {
    StringBuffer queryBuffer = new StringBuffer();
    if (this.queries.size() > 0) {
      for (String query : this.queries) {
        queryBuffer.append(query);
        queryBuffer.append(" ");
      }
      queryBuffer.replace(queryBuffer.length() - 1, queryBuffer.length(), "");
    }

    return queryBuffer.toString();
  }
}
