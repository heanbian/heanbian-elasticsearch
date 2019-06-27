package com.heanbian.block.elasticsearch.client.executor;

import java.io.IOException;

import com.heanbian.block.elasticsearch.client.operator.HOperator;

public class HDefaultExecutor implements HExecutor {

	@Override
	public <E, R, S> S exec(HOperator<E, R, S> operator, R request) {
		try {
			return operator.operator(operator.getRestClient(), request);
		} catch (IOException e) {
			throw new RuntimeException("Elasticsearch连接异常：", e);
		}
	}
}