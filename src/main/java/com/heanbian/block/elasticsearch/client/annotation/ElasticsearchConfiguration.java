package com.heanbian.block.elasticsearch.client.annotation;

import static java.util.Objects.requireNonNull;

import java.util.List;

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

import com.heanbian.block.elasticsearch.client.ConnectionString;
import com.heanbian.block.elasticsearch.client.ElasticsearchTemplate;

@Configuration
public class ElasticsearchConfiguration {

	@Value("${elasticsearch.cluster-nodes:}")
	private String connectionString;

	@Bean
	public RestHighLevelClient restHighLevelClient() {
		requireNonNull(connectionString, "elasticsearch.cluster-nodes is required");

		ConnectionString conn = new ConnectionString(connectionString);
		List<String> nodes = conn.getHosts();
		final int size = nodes.size();
		HttpHost[] hosts = new HttpHost[size];

		for (int i = 0; i < size; i++) {
			String[] s = nodes.get(i).split(":");
			if (s.length == 2) {
				hosts[i] = new HttpHost(s[0], Integer.valueOf(s[1]));
			}
		}

		final CredentialsProvider credentials = new BasicCredentialsProvider();
		credentials.setCredentials(AuthScope.ANY,
				new UsernamePasswordCredentials(conn.getUsername(), conn.getPassword()));

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
		return new ElasticsearchTemplate(restHighLevelClient());
	}

}
