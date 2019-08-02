package com.heanbian.block.reactive.elasticsearch.client.annotation;

import java.util.Objects;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.heanbian.block.reactive.elasticsearch.client.ElasticsearchTemplate;

@Configuration
public class ElasticsearchConfiguration {

	@Value("${elasticsearch.cluster-nodes:}")
	private String clusterNodes;

	@Bean
	public RestClientBuilder restClientBuilder() {
		Objects.requireNonNull(clusterNodes, "elasticsearch.cluster-nodes must be set");
		String[] nodes = clusterNodes.split(",");
		HttpHost[] hosts = new HttpHost[nodes.length];
		for (int i = 0; i < nodes.length; i++) {
			String[] s = nodes[i].split(":");
			if (s.length == 2) {
				hosts[i] = new HttpHost(s[0], Integer.valueOf(s[1]));
			}
		}
		return RestClient.builder(hosts);
	}

	@Bean
	public ElasticsearchTemplate elasticsearchTemplate() {
		return new ElasticsearchTemplate();
	}

}
