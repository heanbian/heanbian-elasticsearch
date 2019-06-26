package net.dgg.framework.tac.elasticsearch.core.operator;

import org.elasticsearch.client.RestHighLevelClient;

public abstract class DggESHighLevelOpertor<R, S> extends DggAbstractOpertor<RestHighLevelClient, R, S> {

	@Override
	public RestHighLevelClient getRestClient() {
		return DggESHighLevelClient.getInstance().getRestHighLevelClient();
	}
}