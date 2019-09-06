package com.heanbian.block.reactive.elasticsearch.client.annotation;

import java.util.Objects;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder.HttpClientConfigCallback;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.heanbian.block.reactive.elasticsearch.client.ElasticsearchTemplate;

@Configuration
public class ElasticsearchConfiguration {

	@Value("${elasticsearch.cluster-nodes:}")
	private String clusterNodes;

	@Value("${elasticsearch.username:}")
	private String username;

	@Value("${elasticsearch.password:}")
	private String password;

	@Bean
	public RestHighLevelClient restHighLevelClient() {
		Objects.requireNonNull(clusterNodes, "elasticsearch.cluster-nodes must be set");

		String[] nodes = clusterNodes.split(",");
		HttpHost[] hosts = new HttpHost[nodes.length];
		for (int i = 0; i < nodes.length; i++) {
			String[] s = nodes[i].split(":");
			if (s.length == 2) {
				hosts[i] = new HttpHost(s[0], Integer.valueOf(s[1]));
			}
		}

		final CredentialsProvider credentials = new BasicCredentialsProvider();
		credentials.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));

		return new RestHighLevelClient(
				RestClient.builder(hosts).setHttpClientConfigCallback(new HttpClientConfigCallback() {
					@Override
					public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
						return httpClientBuilder.setDefaultCredentialsProvider(credentials);
					}
				}));
	}

	@Bean
	public ElasticsearchTemplate elasticsearchTemplate() {
		return new ElasticsearchTemplate();
	}

}
