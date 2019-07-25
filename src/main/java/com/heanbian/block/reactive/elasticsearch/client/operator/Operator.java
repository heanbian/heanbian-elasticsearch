package com.heanbian.block.reactive.elasticsearch.client.operator;

import java.io.IOException;

import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClientBuilder;

public interface Operator<E, R, S> {

	E getRestClient();

	S operator(E client, R request) throws IOException;

	RestClientBuilder getBuilder();

	void setBuilder(RestClientBuilder builder);

	RequestOptions getRequestOptions();

	void setRequestOptions(RequestOptions requestOptions);
}
