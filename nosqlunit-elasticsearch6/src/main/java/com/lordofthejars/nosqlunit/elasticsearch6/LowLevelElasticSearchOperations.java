package com.lordofthejars.nosqlunit.elasticsearch6;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

public class LowLevelElasticSearchOperations {
	private static final int NUM_RETRIES_TO_CHECK_SERVER_UP = 3;

	public boolean assertThatConnectionToElasticsearchIsPossible(final Settings settings, final String host,
			final int port) throws InterruptedException {
		final InetSocketAddress address = new InetSocketAddress(host, port);

		try (TransportClient transportClient = new PreBuiltTransportClient(settings)) {
			transportClient.addTransportAddress(new TransportAddress(address));
			for (int i = 0; i < NUM_RETRIES_TO_CHECK_SERVER_UP; i++) {
				try {
					transportClient.admin().cluster().prepareState().execute().actionGet();
					return true;
				} catch (Exception e) {
					TimeUnit.SECONDS.sleep(7);
				}
			}
		}

		return false;
	}
}
