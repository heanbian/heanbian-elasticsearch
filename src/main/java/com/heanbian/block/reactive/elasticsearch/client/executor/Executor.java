package com.heanbian.block.reactive.elasticsearch.client.executor;

import com.heanbian.block.reactive.elasticsearch.client.operator.Operator;

public interface Executor {

	<E, R, S> S exec(Operator<E, R, S> operator, R request);

}
