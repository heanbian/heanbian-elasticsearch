package net.dgg.framework.tac.elasticsearch.annotation;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@ComponentScan("net.dgg.framework.tac.elasticsearch")
@Configuration
public class ElasticsearchRestClientBuilderConfiguration {

	@Value("${es.cluster.nodes:}")
	private String cluster_nodes;
	private HttpHost[] hosts;

	@Bean
	public RestClientBuilder getRestClientBuilder() {
		String[] es_cluster_list = cluster_nodes.split(",");
		hosts = new HttpHost[es_cluster_list.length];
		for (int i = 0; i < es_cluster_list.length; i++) {
			String ip = es_cluster_list[i].split(":")[0];
			Integer port = Integer.valueOf(es_cluster_list[i].split(":")[1]);
			hosts[i] = new HttpHost(ip, port);
		}
		return RestClient.builder(hosts);
	}

}
