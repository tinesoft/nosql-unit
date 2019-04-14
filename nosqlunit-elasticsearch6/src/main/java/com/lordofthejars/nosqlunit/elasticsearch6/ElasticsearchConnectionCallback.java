package com.lordofthejars.nosqlunit.elasticsearch6;

import org.elasticsearch.client.Client;

public interface ElasticsearchConnectionCallback {
	Client nodeClient();
}
