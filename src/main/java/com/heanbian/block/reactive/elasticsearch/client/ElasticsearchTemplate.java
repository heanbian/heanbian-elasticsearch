package com.heanbian.block.reactive.elasticsearch.client;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.GetAliasesResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;

import com.alibaba.fastjson.JSON;
import com.heanbian.block.reactive.elasticsearch.client.annotation.ElasticsearchId;
import com.heanbian.block.reactive.elasticsearch.client.executor.Executor;
import com.heanbian.block.reactive.elasticsearch.client.executor.ExecutorImpl;
import com.heanbian.block.reactive.elasticsearch.client.operator.Operator;
import com.heanbian.block.reactive.elasticsearch.client.page.PageResult;

public class ElasticsearchTemplate implements InitializingBean {

	private Executor executor;
	private GetOperator operator;
	private CreateIndexOperator createIndexOperator;
	private BulkOperator bulkOperator;
	private SearchOperator searchOperator;
	private SearchScrollOperator searchScrollOperator;
	private ClearScrollOperator clearScrollOperator;
	private IndicesExistsOperator indicesExistsOperator;
	private AliasesOperator aliasesOperator;

	@Autowired
	private RestHighLevelClient client;

	public <R, S> S exec(Operator<R, S> operator, R request) {
		return executor.exec(operator, request);
	}

	public class CreateIndexOperator implements Operator<CreateIndexRequest, CreateIndexResponse> {

		@Override
		public CreateIndexResponse operator(CreateIndexRequest request) throws IOException {
			return client.indices().create(request, RequestOptions.DEFAULT);
		}
	}

	public class BulkOperator implements Operator<BulkRequest, BulkResponse> {

		@Override
		public BulkResponse operator(BulkRequest request) throws IOException {
			return client.bulk(request, RequestOptions.DEFAULT);
		}
	}

	public class GetOperator implements Operator<GetRequest, GetResponse> {

		@Override
		public GetResponse operator(GetRequest request) throws IOException {
			return client.get(request, RequestOptions.DEFAULT);
		}
	}

	public class SearchOperator implements Operator<SearchRequest, SearchResponse> {

		@Override
		public SearchResponse operator(SearchRequest request) throws IOException {
			return client.search(request, RequestOptions.DEFAULT);
		}
	}

	public class SearchScrollOperator implements Operator<SearchScrollRequest, SearchResponse> {

		@Override
		public SearchResponse operator(SearchScrollRequest request) throws IOException {
			return client.scroll(request, RequestOptions.DEFAULT);
		}
	}

	public class ClearScrollOperator implements Operator<ClearScrollRequest, ClearScrollResponse> {

		@Override
		public ClearScrollResponse operator(ClearScrollRequest request) throws IOException {
			return client.clearScroll(request, RequestOptions.DEFAULT);
		}
	}

	public class IndicesExistsOperator implements Operator<GetIndexRequest, Boolean> {

		@Override
		public Boolean operator(GetIndexRequest request) throws IOException {
			return client.indices().exists(request, RequestOptions.DEFAULT);
		}
	}

	public class AliasesOperator implements Operator<GetAliasesRequest, GetAliasesResponse> {

		@Override
		public GetAliasesResponse operator(GetAliasesRequest request) throws IOException {
			return client.indices().getAlias(request, RequestOptions.DEFAULT);
		}
	}

	public Set<String> getAliases() {
		return exec(aliasesOperator, new GetAliasesRequest()).getAliases().keySet();
	}

	public boolean indicesExists(String... indices) {
		Objects.requireNonNull(indices, "indices must not be null");
		GetIndexRequest request = new GetIndexRequest(indices);
		return exec(indicesExistsOperator, request);
	}

	public CreateIndexResponse createIndex(String index, int shards, int replicas) {
		return createIndex(index, shards, replicas, null, null);
	}

	public CreateIndexResponse createIndex(String index, int shards, int replicas, Map<String, ?> mapping) {
		return createIndex(index, shards, replicas, mapping, null);
	}

	public CreateIndexResponse createIndex(String index, int shards, int replicas, Map<String, ?> mapping,
			Map<String, ?> alias) {
		CreateIndexRequest request = new CreateIndexRequest(index);
		request.settings(
				Settings.builder().put("index.number_of_shards", shards).put("index.number_of_replicas", replicas));
		if (mapping != null) {
			request.mapping(mapping);
		}
		if (alias != null) {
			request.aliases(alias);
		}
		return exec(createIndexOperator, request);
	}

	public <T> BulkResponse bulkInsert(String index, T source) {
		return bulkInsert(index, Arrays.asList(source));
	}

	public <T> BulkResponse bulkInsert(String index, List<T> sources) {
		Objects.requireNonNull(index, "index must not be null");
		Objects.requireNonNull(sources, "sources must not be null");

		BulkRequest request = new BulkRequest();
		sources.forEach(d -> {
			request.add(new IndexRequest(index).id(id(d)).source(JSON.toJSONString(d), XContentType.JSON));
		});
		return exec(bulkOperator, request);
	}

	public BulkResponse bulkDelete(String index, String... ids) {
		return bulkDelete(index, Arrays.asList(ids));
	}

	public BulkResponse bulkDelete(String index, List<String> ids) {
		Objects.requireNonNull(index, "index must not be null");
		Objects.requireNonNull(ids, "ids must not be null");

		BulkRequest request = new BulkRequest();
		ids.forEach(id -> {
			request.add(new DeleteRequest(index, id));
		});
		return exec(bulkOperator, request);
	}

	public GetResponse findById(String index, String id) {
		Objects.requireNonNull(index, "index must not be null");
		Objects.requireNonNull(id, "id must not be null");

		return exec(operator, new GetRequest(index, id));
	}

	public <T> T findById(String index, String id, Class<T> clazz) {
		Objects.requireNonNull(clazz, "clazz must not be null");

		GetResponse response = findById(index, id);
		return JSON.parseObject(response.getSourceAsString(), clazz);
	}

	public <T> BulkResponse bulkUpdate(String index, T source) {
		return bulkUpdate(index, Arrays.asList(source));
	}

	public <T> BulkResponse bulkUpdate(String index, List<T> sources) {
		Objects.requireNonNull(index, "index must not be null");
		Objects.requireNonNull(sources, "sources must not be null");

		BulkRequest request = new BulkRequest();
		sources.forEach(d -> {
			request.add(new UpdateRequest(index, id(d)).doc(JSON.toJSONString(d), XContentType.JSON));
		});
		return exec(bulkOperator, request);
	}

	public SearchResponse search(SearchSourceBuilder searchSourceBuilder, String... indices) {
		return search(searchSourceBuilder, "1m", indices);
	}

	public <T> List<T> search(SearchSourceBuilder searchSourceBuilder, String[] indices, Class<T> clazz) {
		Objects.requireNonNull(searchSourceBuilder, "searchSourceBuilder must not be null");
		Objects.requireNonNull(indices, "indices must not be null");
		Objects.requireNonNull(clazz, "clazz must not be null");

		SearchResponse response = search(searchSourceBuilder, "1m", indices);
		List<T> rs = new ArrayList<>();
		SearchHit[] hits = response.getHits().getHits();
		for (SearchHit h : hits) {
			rs.add(JSON.parseObject(h.getSourceAsString(), clazz));
		}
		return rs;
	}

	public SearchResponse search(SearchSourceBuilder searchSourceBuilder, String keepAlive, String[] indices) {
		Objects.requireNonNull(searchSourceBuilder, "searchSourceBuilder must not be null");
		Objects.requireNonNull(indices, "indices must not be null");

		SearchRequest request = new SearchRequest(indices);
		request.source(searchSourceBuilder);
		if (keepAlive == null) {
			keepAlive = "1m";
		}
		request.scroll(keepAlive);
		return exec(searchOperator, request);
	}

	public SearchResponse searchScroll(String scrollId) {
		return searchScroll(scrollId, null);
	}

	public SearchResponse searchScroll(String scrollId, String keepAlive) {
		Objects.requireNonNull(scrollId, "scrollId must not be null");

		SearchScrollRequest request = new SearchScrollRequest(scrollId);
		if (keepAlive != null) {
			request.scroll(keepAlive);
		}
		return exec(searchScrollOperator, request);
	}

	public ClearScrollResponse clearScroll(String... scrollId) {
		return clearScroll(Arrays.asList(scrollId));
	}

	public ClearScrollResponse clearScroll(List<String> scrollIds) {
		Objects.requireNonNull(scrollIds, "scrollIds must not be null");

		ClearScrollRequest request = new ClearScrollRequest();
		request.scrollIds(scrollIds);
		return exec(clearScrollOperator, request);
	}

	private <T> String id(T source) {
		String id = "";
		try {
			loop: for (Field f : source.getClass().getDeclaredFields()) {
				if (f.isAnnotationPresent(ElasticsearchId.class)) {
					f.setAccessible(true);
					if (f.get(source) != null) {
						id = f.get(source).toString();
						break loop;
					}
				}
			}
		} catch (Exception e) {// Ignore
		}
		return id;
	}

	public <T> PageResult<T> searchScrollDeepPaging(SearchSourceBuilder searchSourceBuilder, int pageNumber,
			int pageSize, String index, String keepAlive, Class<T> clazz) {
		return searchScrollDeepPaging(searchSourceBuilder, pageNumber, pageSize, new String[] { index }, keepAlive,
				clazz);
	}

	public <T> PageResult<T> searchScrollDeepPaging(SearchSourceBuilder searchSourceBuilder, final int pageNumber,
			final int pageSize, final String[] indices, final String keepAlive, Class<T> clazz) {

		Objects.requireNonNull(searchSourceBuilder, "searchSourceBuilder must not be null");
		Objects.requireNonNull(indices, "indices must not be null");
		Objects.requireNonNull(clazz, "clazz must not be null");

		searchSourceBuilder.from(0).size(pageSize);// scroll from=0
		SearchResponse response = search(searchSourceBuilder, keepAlive, indices);

		final long total = response.getHits().getTotalHits().value;
		List<String> scrollIds = new ArrayList<>();
		List<T> tss = new ArrayList<>();

		loop: for (int i = 0; i < pageNumber; i++) {
			SearchHit[] hits = response.getHits().getHits();
			if (hits == null || hits.length <= 0) {
				break loop;
			}
			if (i == (pageNumber - 1)) {
				for (SearchHit hit : hits) {
					tss.add(JSON.parseObject(hit.getSourceAsString(), clazz));
				}
				break loop;
			}
			scrollIds.add(response.getScrollId());
			response = searchScroll(response.getScrollId(), keepAlive);
		}
		if (scrollIds.size() > 0) {
			clearScroll(scrollIds);
		}

		return new PageResult<T>().setList(tss).setPageNumber(pageNumber).setPageSize(pageSize).setTotal(total);
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		executor = new ExecutorImpl();
		operator = new GetOperator();
		createIndexOperator = new CreateIndexOperator();
		bulkOperator = new BulkOperator();
		searchOperator = new SearchOperator();
		searchScrollOperator = new SearchScrollOperator();
		clearScrollOperator = new ClearScrollOperator();
		indicesExistsOperator = new IndicesExistsOperator();
		aliasesOperator = new AliasesOperator();
	}

}