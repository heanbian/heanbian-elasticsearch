package com.heanbian.block.reactive.elasticsearch.client.operator;

import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

@Component
public class ElasticsearchHighLevelClient implements ApplicationContextAware {

	private RestHighLevelClient restHighLevelClient;

	@Autowired
	private RestClientBuilder restClientBuilder;

	private static ElasticsearchHighLevelClient client;

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		restHighLevelClient = new RestHighLevelClient(restClientBuilder);
		client = this;
	}

	public final static ElasticsearchHighLevelClient getInstance() {
		return client;
	}

	public RestHighLevelClient getRestHighLevelClient() {
		return restHighLevelClient;
	}

	public void setRestHighLevelClient(RestHighLevelClient restHighLevelClient) {
		this.restHighLevelClient = restHighLevelClient;
	}

	public RestClientBuilder getRestClientBuilder() {
		return restClientBuilder;
	}

	public void setRestClientBuilder(RestClientBuilder restClientBuilder) {
		this.restClientBuilder = restClientBuilder;
	}
}