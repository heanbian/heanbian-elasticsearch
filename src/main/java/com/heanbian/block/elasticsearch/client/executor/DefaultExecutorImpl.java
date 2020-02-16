package com.heanbian.block.elasticsearch.client.executor;

import java.io.IOException;

import com.heanbian.block.elasticsearch.client.operator.Operator;

public class DefaultExecutorImpl implements Executor {

	@Override
	public <R, S> S exec(Operator<R, S> operator, R request) {
		try {
			return operator.operator(request);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}