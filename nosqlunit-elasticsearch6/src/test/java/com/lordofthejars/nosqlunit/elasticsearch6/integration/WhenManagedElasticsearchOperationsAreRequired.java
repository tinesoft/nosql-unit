package com.lordofthejars.nosqlunit.elasticsearch6.integration;

import static com.lordofthejars.nosqlunit.elasticsearch6.ManagedElasticsearch.ManagedElasticsearchRuleBuilder.newManagedElasticsearchRule;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.collection.IsMapContaining.hasEntry;
import static org.junit.Assert.assertThat;

import java.io.ByteArrayInputStream;
import java.net.InetSocketAddress;
import java.util.Map;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import com.lordofthejars.nosqlunit.elasticsearch6.ElasticsearchOperation;
import com.lordofthejars.nosqlunit.elasticsearch6.ManagedElasticsearch;

public class WhenManagedElasticsearchOperationsAreRequired {
	private static final String ELASTICSEARCH_DATA = "{\n" +
			"   \"documents\":[\n" +
			"	  {\n" +
			"		 \"document\":[\n" +
			"			{\n" +
			"			   \"index\":{\n" +
			"				  \"indexName\":\"tweeter\",\n" +
			"				  \"indexType\":\"tweet\",\n" +
			"				  \"indexId\":\"1\"\n" +
			"			   }\n" +
			"			},\n" +
			"			{\n" +
			"			   \"data\":{\n" +
			"				  \"name\":\"a\",\n" +
			"				  \"msg\":\"b\"\n" +
			"			   }\n" +
			"			}\n" +
			"		 ]\n" +
			"	  }\n" +
			"   ]\n" +
			"}";


	@ClassRule
	public static ManagedElasticsearch managedElasticsearch = newManagedElasticsearchRule().elasticsearchPath("/usr/local/Cellar/elasticsearch/6.7.1/").build();

	private static final InetSocketAddress INET_SOCKET_ADDRESS = new InetSocketAddress("localhost", 9300);
	private static final TransportAddress TRANSPORT_ADDRESS = new TransportAddress(INET_SOCKET_ADDRESS);

	private Client client;

	@Before
	public void setupClient() {
		client = new PreBuiltTransportClient(Settings.EMPTY).addTransportAddress(TRANSPORT_ADDRESS);
	}


	@After
	public void removeIndexes() {
		client.admin().indices().prepareDelete("*").execute().actionGet();
		client.admin().indices().prepareRefresh().execute().actionGet();

		client.close();
	}

	@Test
	public void insert_operation_should_index_all_dataset() {
		ElasticsearchOperation elasticsearchOperation = new ElasticsearchOperation(client);
		elasticsearchOperation.insert(new ByteArrayInputStream(ELASTICSEARCH_DATA.getBytes()));

		GetResponse document = client.prepareGet("tweeter", "tweet", "1").execute().actionGet();
		Map<String, Object> documentSource = document.getSource();

		//Strange a cast to Object
		assertThat(documentSource, hasEntry("name", (Object) "a"));
		assertThat(documentSource, hasEntry("msg", (Object) "b"));
	}

	@Test
	public void delete_operation_should_remove_all_Indexes() {
		ElasticsearchOperation elasticsearchOperation = new ElasticsearchOperation(client);
		elasticsearchOperation.insert(new ByteArrayInputStream(ELASTICSEARCH_DATA.getBytes()));

		elasticsearchOperation.deleteAll();

		GetResponse document = client.prepareGet("tweeter", "tweet", "1").execute().actionGet();
		assertThat(document.isSourceEmpty(), is(true));
	}

	@Test
	public void databaseIs_operation_should_compare_all_Indexes() {
		ElasticsearchOperation elasticsearchOperation = new ElasticsearchOperation(client);
		elasticsearchOperation.insert(new ByteArrayInputStream(ELASTICSEARCH_DATA.getBytes()));

		boolean isEqual = elasticsearchOperation.databaseIs(new ByteArrayInputStream(ELASTICSEARCH_DATA.getBytes()));

		assertThat(isEqual, is(true));
	}
}
