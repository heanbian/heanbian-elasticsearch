package com.heanbian.block.reactive.elasticsearch.client.operator;

import org.elasticsearch.client.RestHighLevelClient;

public abstract class HighLevelOperator<R, S> extends AbstractOperator<RestHighLevelClient, R, S> {

	@Override
	public RestHighLevelClient getRestClient() {
		return ElasticsearchHighLevelClient.getInstance().getRestHighLevelClient();
	}
}