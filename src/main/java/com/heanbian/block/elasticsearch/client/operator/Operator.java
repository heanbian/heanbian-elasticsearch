package com.heanbian.block.elasticsearch.client.operator;

import java.io.IOException;

@FunctionalInterface
public interface Operator<R, S> {

	S operator(R request) throws IOException;

}
