package com.heanbian.block.reactive.elasticsearch.client.operator;

import java.io.IOException;

@FunctionalInterface
public interface Operator<R, S> {

	S operator(R request) throws IOException;

}
