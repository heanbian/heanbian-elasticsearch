package com.heanbian.block.reactive.elasticsearch.client.executor;

import java.io.IOException;

import com.heanbian.block.reactive.elasticsearch.client.operator.Operator;

public class DefaultExecutor implements Executor {

	@Override
	public <E, R, S> S exec(Operator<E, R, S> operator, R request) {
		try {
			return operator.operator(operator.getRestClient(), request);
		} catch (IOException e) {
			throw new RuntimeException("Elasticsearch连接异常：", e);
		}
	}
}