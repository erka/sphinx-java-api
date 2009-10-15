package org.sphx.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JUnitProperties {
	private static final String SPHINX_SEARCHERD_KEY = "sphinx.searchd";
	private static final String SPHINX_INDEXER_KEY = "sphinx.indexer";

	private static final String SPHINX_SEARCHERD_DEFAULT_CMD = "searchd";
	private static final String SPHINX_INDEXER_DEFAULT_CMD = "indexer";

	private static Logger logger = LoggerFactory.getLogger(JUnitProperties.class);
	Properties properties = new Properties();

	public JUnitProperties() {

		InputStream propertiesInputStream = getClass().getResourceAsStream("/junit.properties");
		if (propertiesInputStream != null) {
			try {
				properties.load(propertiesInputStream);
			} catch (IOException ex) {
				logger.warn("Located junit.properties but could not load from it", ex);
			}
		} else {
			logger.info("junit.properties was not found. Use default configuration");
		}
	}

	public String getSphinxSearcherd() {
		String searcherd = properties.getProperty(SPHINX_SEARCHERD_KEY);
		if (searcherd == null) {
			searcherd = SPHINX_SEARCHERD_DEFAULT_CMD;
		}
		return searcherd;
	}

	public String getSphinxIndexer() {
		String indexer = properties.getProperty(SPHINX_INDEXER_KEY);
		if (indexer == null) {
			indexer = SPHINX_INDEXER_DEFAULT_CMD;
		}
		return indexer;
	}
}