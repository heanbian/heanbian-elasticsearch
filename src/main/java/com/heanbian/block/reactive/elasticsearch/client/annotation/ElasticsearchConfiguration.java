package com.heanbian.block.reactive.elasticsearch.client.annotation;

import java.util.Objects;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@ComponentScan("com.heanbian.block.reactive.elasticsearch.client")
@Configuration
public class ElasticsearchConfiguration {

	@Value("${elasticsearch.cluster-nodes:}")
	private String cluster_nodes;
	private HttpHost[] hosts;

	@Bean
	public RestClientBuilder getRestClientBuilder() {
		Objects.requireNonNull(cluster_nodes, "elasticsearch.cluster-nodes must not be null");
		String[] nodes = cluster_nodes.split(",");
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
