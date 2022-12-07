package com.heanbian.block.elasticsearch.client;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch.core.ClearScrollRequest;
import co.elastic.clients.elasticsearch.core.ClearScrollResponse;
import co.elastic.clients.elasticsearch.core.CountRequest;
import co.elastic.clients.elasticsearch.core.CountResponse;
import co.elastic.clients.elasticsearch.core.CreateRequest;
import co.elastic.clients.elasticsearch.core.CreateResponse;
import co.elastic.clients.elasticsearch.core.DeleteByQueryRequest;
import co.elastic.clients.elasticsearch.core.DeleteByQueryResponse;
import co.elastic.clients.elasticsearch.core.ExistsRequest;
import co.elastic.clients.elasticsearch.core.ExplainRequest;
import co.elastic.clients.elasticsearch.core.ExplainResponse;
import co.elastic.clients.elasticsearch.core.GetRequest;
import co.elastic.clients.elasticsearch.core.GetResponse;
import co.elastic.clients.elasticsearch.core.ScrollRequest;
import co.elastic.clients.elasticsearch.core.ScrollResponse;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.UpdateByQueryRequest;
import co.elastic.clients.elasticsearch.core.UpdateByQueryResponse;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import co.elastic.clients.elasticsearch.indices.CreateIndexResponse;
import co.elastic.clients.elasticsearch.indices.DeleteIndexRequest;
import co.elastic.clients.elasticsearch.indices.DeleteIndexResponse;
import co.elastic.clients.elasticsearch.indices.GetAliasRequest;
import co.elastic.clients.transport.endpoints.BooleanResponse;

public class ElasticsearchTemplate {

	private final ElasticsearchClient client;

	public ElasticsearchTemplate(String connectionString) {
		this(new ConnectionString(connectionString));
	}

	public ElasticsearchTemplate(ConnectionString connectionString) {
		this(connectionString.getElasticsearchClient());
	}

	public ElasticsearchTemplate(ElasticsearchClient client) {
		this.client = client;
	}

	public ElasticsearchClient getElasticsearchClient() {
		return client;
	}

	public CountResponse count(Query q, String... index) {
		return count(CountRequest.of(b -> b.index(List.of(index)).query(q)));
	}

	public CountResponse count(CountRequest req) {
		try {
			return client.count(req);
		} catch (ElasticsearchException | IOException e) {
			throw new RuntimeException(e);
		}
	}

	public BooleanResponse exists(String id, String index) {
		try {
			return client.exists(ExistsRequest.of(b -> b.index(index).id(id)));
		} catch (ElasticsearchException | IOException e) {
			throw new RuntimeException(e);
		}
	}

	public DeleteByQueryResponse deleteByQuery(Query q, String... index) {
		return deleteByQuery(DeleteByQueryRequest.of(b -> b.index(List.of(index)).query(q)));
	}

	public DeleteByQueryResponse deleteByQuery(DeleteByQueryRequest req) {
		try {
			return client.deleteByQuery(req);
		} catch (ElasticsearchException | IOException e) {
			throw new RuntimeException(e);
		}
	}

	public UpdateByQueryResponse updateByQuery(Query q, String... index) {
		return updateByQuery(UpdateByQueryRequest.of(b -> b.index(List.of(index)).query(q)));
	}

	public UpdateByQueryResponse updateByQuery(UpdateByQueryRequest req) {
		try {
			return client.updateByQuery(req);
		} catch (ElasticsearchException | IOException e) {
			throw new RuntimeException(e);
		}
	}

	public CreateIndexResponse createIndex(String index) {
		try {
			return client.indices().create(CreateIndexRequest.of(b -> b.index(index)));
		} catch (ElasticsearchException | IOException e) {
			throw new RuntimeException(e);
		}
	}

	public DeleteIndexResponse deleteIndex(String index) {
		try {
			return client.indices().delete(DeleteIndexRequest.of(b -> b.index(index)));
		} catch (ElasticsearchException | IOException e) {
			throw new RuntimeException(e);
		}
	}

	public <T> ExplainResponse<T> explain(Query q, String index, Class<T> clazz) {
		try {
			return client.explain(ExplainRequest.of(b -> b.index(index).query(q)), clazz);
		} catch (ElasticsearchException | IOException e) {
			throw new RuntimeException(e);
		}
	}

	public <T> GetResponse<T> get(String id, String index, Class<T> clazz) {
		try {
			return client.get(GetRequest.of(b -> b.index(index).id(id)), clazz);
		} catch (ElasticsearchException | IOException e) {
			throw new RuntimeException(e);
		}
	}

	public <T> CreateResponse create(String id, String index, T doc) {
		try {
			return client.create(CreateRequest.of(b -> b.id(id).index(index).document(doc)));
		} catch (ElasticsearchException | IOException e) {
			throw new RuntimeException(e);
		}
	}

	public <T> SearchResponse<T> search(SearchRequest request, Class<T> clazz) {
		try {
			return client.search(request, clazz);
		} catch (ElasticsearchException | IOException e) {
			throw new RuntimeException(e);
		}
	}

	public <T> ScrollResponse<T> scroll(String scrollId, Class<T> clazz) {
		try {
			return client.scroll(ScrollRequest.of(b -> b.scrollId(scrollId)), clazz);
		} catch (ElasticsearchException | IOException e) {
			throw new RuntimeException(e);
		}
	}

	public ClearScrollResponse clearScroll(List<String> scrollIds) {
		try {
			return client.clearScroll(ClearScrollRequest.of(b -> b.scrollId(scrollIds)));
		} catch (ElasticsearchException | IOException e) {
			throw new RuntimeException(e);
		}
	}

	public Set<String> getAliases() {
		try {
			return client.indices().getAlias(new GetAliasRequest.Builder().build()).result().keySet();
		} catch (ElasticsearchException | IOException e) {
			throw new RuntimeException(e);
		}
	}

	public <T> T findById(String index, String id, Class<T> clazz) {
		return get(id, index, clazz).source();
	}

}