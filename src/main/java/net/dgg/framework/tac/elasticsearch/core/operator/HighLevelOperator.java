package net.dgg.framework.tac.elasticsearch.core.operator;

import org.elasticsearch.client.RestHighLevelClient;

public abstract class HighLevelOperator<R, S> extends HAbstractOperator<RestHighLevelClient, R, S> {

	@Override
	public RestHighLevelClient getRestClient() {
		return HElasticsearchHighLevelClient.getInstance().getRestHighLevelClient();
	}
}