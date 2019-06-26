package com.heanbian.block.elasticsearch.client.annotation;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@ComponentScan("com.heanbian.block.elasticsearch.client")
@Configuration
public class ElasticsearchRestClientBuilderConfiguration {

	@Value("${elasticsearch.cluster.node:}")
	private String cluster_node;
	private HttpHost[] hosts;

	@Bean
	public RestClientBuilder getRestClientBuilder() {
		String[] nodes = cluster_node.split(",");
		hosts = new HttpHost[nodes.length];
		for (int i = 0; i < nodes.length; i++) {
			String[] s = nodes[i].split(":");
			if (s.length == 2) {
				hosts[i] = new HttpHost(s[0], Integer.valueOf(s[1]));
			}
		}
		return RestClient.builder(hosts);
	}

}
