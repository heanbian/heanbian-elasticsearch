package com.heanbian.block.elasticsearch.client.executor;

import com.heanbian.block.elasticsearch.client.operator.Operator;

@FunctionalInterface
public interface Executor {

	<R, S> S exec(Operator<R, S> operator, R request);

}
