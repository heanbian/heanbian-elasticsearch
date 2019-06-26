package com.heanbian.block.elasticsearch.client.executor;

import java.io.IOException;

import com.heanbian.block.elasticsearch.client.operator.HOperator;

public class HRetryExecutor implements HExecutor {
	private int retryNumber;

	public HRetryExecutor(int retryNumber) {
		this.retryNumber = retryNumber;
	}

	public int getRetryNumber() {
		return retryNumber;
	}

	public void setRetryNumber(int retryNumber) {
		this.retryNumber = retryNumber;
	}

	@Override
	public <E, R, S> S exec(HOperator<E, R, S> operator, R request) {
		try {
			for (int count = retryNumber; count >= 0;) {
				--count;
				return operator.operator(operator.getRestClient(), request);
			}
		} catch (IOException e) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e1) {
			}
		}
		throw new RuntimeException("Elasticsearch连接异常");
	}
}