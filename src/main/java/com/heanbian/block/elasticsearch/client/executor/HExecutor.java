package com.heanbian.block.elasticsearch.client.executor;

import com.heanbian.block.elasticsearch.client.operator.HOperator;

public interface HExecutor {

	public <E, R, S> S exec(HOperator<E, R, S> operator, R request);

}
