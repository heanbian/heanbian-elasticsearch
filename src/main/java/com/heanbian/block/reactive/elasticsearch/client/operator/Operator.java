package com.heanbian.block.reactive.elasticsearch.client.operator;

import java.io.IOException;

public interface Operator<R, S> {

	S operator(R request) throws IOException;

}
