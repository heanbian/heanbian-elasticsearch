package com.heanbian.block.elasticsearch.client.executor;

import java.io.IOException;

import com.heanbian.block.elasticsearch.client.operator.Operator;

public class DefaultExecutorImpl implements Executor {

	private int retryCount;

	public DefaultExecutorImpl(int retryCount) {
		this.retryCount = retryCount;
	}

	@Override
	public <R, S> S exec(Operator<R, S> operator, R request) {
		IOException internal = null;
		for (int i = 0; i < retryCount; i++) {
			try {
				return operator.operator(request);
			} catch (IOException e) {
				internal = e;
				try {
					Thread.sleep(1000L);
				} catch (InterruptedException e1) {
				}
			}
		}
		throw new RuntimeException(internal);
	}
}