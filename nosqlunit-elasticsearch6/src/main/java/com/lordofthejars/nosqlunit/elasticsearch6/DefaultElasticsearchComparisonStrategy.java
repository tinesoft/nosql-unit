package com.lordofthejars.nosqlunit.elasticsearch6;

import com.lordofthejars.nosqlunit.core.NoSqlAssertionError;
import com.lordofthejars.nosqlunit.elasticsearch6.parser.DataReader;

import org.elasticsearch.client.Client;

import java.io.InputStream;
import java.util.List;
import java.util.Map;

public class DefaultElasticsearchComparisonStrategy implements ElasticsearchComparisonStrategy {
	@Override
	public boolean compare(ElasticsearchConnectionCallback connection, InputStream dataset) throws NoSqlAssertionError,
			Throwable {
		final Client nodeClient = connection.nodeClient();
		final List<Map<String, Object>> documents = DataReader.getDocuments(dataset);
		ElasticsearchAssertion.strictAssertEquals(documents, nodeClient);
		return true;
	}

	@Override
	public void setIgnoreProperties(String[] ignoreProperties) {
	}
}
