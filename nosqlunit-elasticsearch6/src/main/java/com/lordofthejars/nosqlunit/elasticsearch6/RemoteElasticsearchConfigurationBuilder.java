package com.lordofthejars.nosqlunit.elasticsearch6;

import java.net.InetSocketAddress;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

public class RemoteElasticsearchConfigurationBuilder {

	private ElasticsearchConfiguration elasticsearchConfiguration = new ElasticsearchConfiguration();

	private RemoteElasticsearchConfigurationBuilder() {
		super();
	}

	public static RemoteElasticsearchConfigurationBuilder remoteElasticsearch() {
		return new RemoteElasticsearchConfigurationBuilder();
	}

	public RemoteElasticsearchConfigurationBuilder port(final int port) {
		this.elasticsearchConfiguration.setPort(port);
		return this;
	}

	public RemoteElasticsearchConfigurationBuilder host(final String host) {
		this.elasticsearchConfiguration.setHost(host);
		return this;
	}

	public RemoteElasticsearchConfigurationBuilder settings(final Settings settings) {
		this.elasticsearchConfiguration.setSettings(settings);
		return this;
	}

	public RemoteElasticsearchConfigurationBuilder connectionIdentifier(final String connectionIdentifier) {
		this.elasticsearchConfiguration.setConnectionIdentifier(connectionIdentifier);
		return this;
	}

	public ElasticsearchConfiguration build() {
		final InetSocketAddress address = new InetSocketAddress(this.elasticsearchConfiguration.getHost(),
				this.elasticsearchConfiguration.getPort());
		final TransportClient client = new PreBuiltTransportClient(this.elasticsearchConfiguration.getSettings())
				.addTransportAddress(new TransportAddress(address));
		this.elasticsearchConfiguration.setClient(client);

		return this.elasticsearchConfiguration;
	}

}
