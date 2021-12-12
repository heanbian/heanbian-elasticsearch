package com.heanbian.block.elasticsearch.client;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.apache.http.auth.AuthScope.ANY;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.http.HttpHost;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestClientBuilder.HttpClientConfigCallback;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.datatype.jsr310.deser.LocalDateTimeDeserializer;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;

public class ConnectionString {

	private static final String ELASTICSEARCH_PREFIX = "elasticsearch://";
	private String connectionString;
	private String username;
	private String password;
	private List<String> hosts;

	public ConnectionString(final String connectionString) {
		this.connectionString = connectionString;
		boolean isElasticsearchProtocol = connectionString.startsWith(ELASTICSEARCH_PREFIX);
		if (!isElasticsearchProtocol) {
			throw new IllegalArgumentException(
					format("The connection string is invalid. Connection strings must start with either '%s'",
							ELASTICSEARCH_PREFIX));
		}

		String unprocessedConnectionString = connectionString.substring(ELASTICSEARCH_PREFIX.length());

		String hostIdentifier;
		int idx = unprocessedConnectionString.lastIndexOf("@");
		if (idx > 0) {
			String userInfo = unprocessedConnectionString.substring(0, idx).replace("+", "%2B");
			hostIdentifier = unprocessedConnectionString.substring(idx + 1);
			int colonCount = countOccurrences(userInfo, ":");
			if (userInfo.contains("@") || colonCount > 1) {
				throw new IllegalArgumentException("The connection string contains invalid user information. "
						+ "If the username or password contains a colon (:) or an at-sign (@) then it must be urlencoded");
			}
			if (colonCount == 0) {
				this.username = urldecode(userInfo);
			} else {
				idx = userInfo.indexOf(":");
				this.username = urldecode(userInfo.substring(0, idx));
				this.password = urldecode(userInfo.substring(idx + 1), true);
			}
		} else {
			hostIdentifier = unprocessedConnectionString;
		}

		this.hosts = unmodifiableList(parseHosts(asList(hostIdentifier.split(","))));
	}

	public String getUsername() {
		return username;
	}

	public String getPassword() {
		return password;
	}

	public List<String> getHosts() {
		return hosts;
	}

	public String getConnectionString() {
		return connectionString;
	}

	public ElasticsearchClient getElasticsearchClient() {
		ConnectionString conn = this;
		List<String> nodes = conn.getHosts();
		final int size = nodes.size();
		HttpHost[] hosts = new HttpHost[size];

		for (int i = 0; i < size; i++) {
			String[] s = nodes.get(i).split(":");
			if (s.length == 2) {
				hosts[i] = new HttpHost(s[0], Integer.parseInt(s[1]));
			}
		}

		RestClientBuilder rb = RestClient.builder(hosts);
		if (conn.getUsername() != null && conn.getPassword() != null) {
			final CredentialsProvider credentials = new BasicCredentialsProvider();
			credentials.setCredentials(ANY, new UsernamePasswordCredentials(conn.getUsername(), conn.getPassword()));

			rb.setHttpClientConfigCallback(new HttpClientConfigCallback() {
				@Override
				public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
					return httpClientBuilder.setDefaultCredentialsProvider(credentials);
				}
			});
		}

		ElasticsearchTransport transport = new RestClientTransport(rb.build(),
				new JacksonJsonpMapper(defaultObjectMapper()));

		return new ElasticsearchClient(transport);
	}

	private ObjectMapper defaultObjectMapper() {
		DateTimeFormatter f = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

		JavaTimeModule module = new JavaTimeModule();
		module.addDeserializer(LocalDateTime.class, new LocalDateTimeDeserializer(f));

		ObjectMapper om = new ObjectMapper();
		om.registerModules(module);
		om.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
		om.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

		return om;
	}

	private List<String> parseHosts(final List<String> rawHosts) {
		if (rawHosts.size() == 0) {
			throw new IllegalArgumentException("The connection string must contain at least one host");
		}
		List<String> hosts = new ArrayList<String>();
		for (String host : rawHosts) {
			if (host.length() == 0) {
				throw new IllegalArgumentException(
						format("The connection string contains an empty host '%s'. ", rawHosts));
			} else {
				int colonCount = countOccurrences(host, ":");
				if (colonCount > 1) {
					throw new IllegalArgumentException(format(
							"The connection string contains an invalid host '%s'. "
									+ "Reserved characters such as ':' must be escaped according RFC 2396. "
									+ "Any IPv6 address literal must be enclosed in '[' and ']' according to RFC 2732.",
							host));
				} else if (colonCount == 1) {
					validatePort(host, host.substring(host.indexOf(":") + 1));
				}
			}
			hosts.add(host);
		}
		Collections.sort(hosts);
		return hosts;
	}

	private void validatePort(final String host, final String port) {
		boolean invalidPort = false;
		try {
			int portInt = Integer.parseInt(port);
			if (portInt <= 0 || portInt > 65535) {
				invalidPort = true;
			}
		} catch (NumberFormatException e) {
			invalidPort = true;
		}
		if (invalidPort) {
			throw new IllegalArgumentException(format("The connection string contains an invalid host '%s'. "
					+ "The port '%s' is not a valid, it must be an integer between 0 and 65535", host, port));
		}
	}

	private int countOccurrences(final String haystack, final String needle) {
		return haystack.length() - haystack.replace(needle, "").length();
	}

	private String urldecode(final String input) {
		return urldecode(input, false);
	}

	private String urldecode(final String input, final boolean password) {
		try {
			return URLDecoder.decode(input, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			if (password) {
				throw new IllegalArgumentException(
						"The connection string contained unsupported characters in the password.");
			} else {
				throw new IllegalArgumentException(
						format("The connection string contained unsupported characters: '%s'."
								+ "Decoding produced the following error: %s", input, e.getMessage()));
			}
		}
	}
}