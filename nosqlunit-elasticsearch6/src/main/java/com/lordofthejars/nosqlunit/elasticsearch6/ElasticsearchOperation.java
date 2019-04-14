package com.lordofthejars.nosqlunit.elasticsearch6;

import java.io.InputStream;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import com.lordofthejars.nosqlunit.core.AbstractCustomizableDatabaseOperation;
import com.lordofthejars.nosqlunit.core.NoSqlAssertionError;

public class ElasticsearchOperation
		extends AbstractCustomizableDatabaseOperation<ElasticsearchConnectionCallback, Client> {

	private Client client;

	public ElasticsearchOperation(final Client client) {
		this.client = client;
		setInsertionStrategy(new DefaultElasticsearchInsertionStrategy());
		setComparisonStrategy(new DefaultElasticsearchComparisonStrategy());
	}

	@Override
	public void insert(final InputStream dataScript) {
		insertData(dataScript);
	}

	private void insertData(final InputStream dataScript) {
		try {
			executeInsertion(() -> client, dataScript);
		} catch (Throwable e) {
			throw new IllegalArgumentException(e);
		}
	}

	@Override
	public void deleteAll() {
		clearDocuments();
	}

	private void clearDocuments() {
		if (isAnyIndexPresent()) {
			final SearchResponse countResponse = client.prepareSearch().setSearchType(SearchType.QUERY_THEN_FETCH)
					.setQuery(QueryBuilders.matchAllQuery()).setSize(0).execute().actionGet();

			Long docCount = countResponse.getHits().totalHits;
			final SearchResponse scrollResponse = client.prepareSearch().setSearchType(SearchType.DEFAULT)
					.setScroll(new TimeValue(1L, TimeUnit.MINUTES)).setQuery(QueryBuilders.matchAllQuery())
					.setSize(docCount.intValue()).execute().actionGet();

			final BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
			while (true) {
				final SearchResponse searchResponse = client.prepareSearchScroll(scrollResponse.getScrollId())
						.setScroll(new TimeValue(1L, TimeUnit.MINUTES)).execute().actionGet();

				for (SearchHit hit : searchResponse.getHits().getHits()) {
					bulkRequestBuilder.add(client.prepareDelete(hit.getIndex(), hit.getType(), hit.getId()));
				}

				// Break condition: No hits are returned
				if (searchResponse.getHits().getHits().length == 0) {
					break;
				}
			}

			if (bulkRequestBuilder.numberOfActions() > 0) {
				final BulkResponse bulkResponse = bulkRequestBuilder.execute().actionGet();
			}

			refreshNode();
		}

	}

	private boolean isAnyIndexPresent() {
		SearchResponse response = client.prepareSearch().setSource(new SearchSourceBuilder().size(0)).get();
		return response.getHits().totalHits > 0;
	}

	private void refreshNode() {
		client.admin().indices().prepareRefresh().execute().actionGet();
	}

	@Override
	public boolean databaseIs(final InputStream expectedData) {
		try {
			return executeComparison(() -> client, expectedData);
		} catch (NoSqlAssertionError e) {
			throw e;
		} catch (Throwable e) {
			throw new IllegalStateException(e);
		}
	}

	@Override
	public Client connectionManager() {
		return client;
	}
}
