package com.heanbian.block.elasticsearch.client.operator;

import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClientBuilder;

public abstract class HAbstractOperator<E, R, S> implements HOperator<E, R, S> {

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
			restClientBuilder = HElasticsearchHighLevelClient.getInstance().getRestClientBuilder();
		}
		return restClientBuilder;
	}

	@Override
	public void setBuilder(RestClientBuilder restClientBuilder) {
		this.restClientBuilder = restClientBuilder;
	}
}