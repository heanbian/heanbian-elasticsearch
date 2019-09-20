package com.heanbian.block.reactive.elasticsearch.client.executor;

import com.heanbian.block.reactive.elasticsearch.client.operator.Operator;

@FunctionalInterface
public interface Executor {

	<R, S> S exec(Operator<R, S> operator, R request);

}
