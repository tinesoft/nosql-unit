package com.lordofthejars.nosqlunit.elasticsearch6;

import java.io.InputStream;

import com.lordofthejars.nosqlunit.elasticsearch6.parser.DataReader;

public class DefaultElasticsearchInsertionStrategy implements ElasticsearchInsertionStrategy {
	@Override
	public void insert(ElasticsearchConnectionCallback connection, InputStream dataset) throws Throwable {
		DataReader dataReader = new DataReader(connection.nodeClient());
		dataReader.read(dataset);
	}
}
