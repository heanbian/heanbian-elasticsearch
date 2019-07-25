package com.heanbian.block.reactive.elasticsearch.client.operator;

import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClientBuilder;

public abstract class AbstractOperator<E, R, S> implements Operator<E, R, S> {

	private RequestOptions requestOptions = RequestOptions.DEFAULT;
	private RestClientBuilder restClientBuilder;

	@Override
	public RequestOptions getRequestOptions() {
		return requestOptions;
	}

	@Override
	public void setRequestOptions(RequestOptions requestOptions) {
		this.requestOptions = requestOptions;
	}

	@Override
	public RestClientBuilder getBuilder() {
		if (restClientBuilder == null) {
			restClientBuilder = ElasticsearchHighLevelClient.getInstance().getRestClientBuilder();
		}
		return restClientBuilder;
	}

	@Override
	public void setBuilder(RestClientBuilder restClientBuilder) {
		this.restClientBuilder = restClientBuilder;
	}
}