package com.heanbian.block.elasticsearch.client;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

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
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.stereotype.Component;

import com.alibaba.fastjson.JSON;
import com.heanbian.block.elasticsearch.client.annotation.ElasticsearchId;
import com.heanbian.block.elasticsearch.client.executor.HDefaultExecutor;
import com.heanbian.block.elasticsearch.client.executor.HExecutor;
import com.heanbian.block.elasticsearch.client.operator.HOperator;
import com.heanbian.block.elasticsearch.client.operator.HighLevelOperator;
import com.heanbian.block.elasticsearch.client.page.HPageResult;

@Component
public class HElasticsearchTemplate {

	private HExecutor executor;
	private CreateIndexOperator createIndexOperator;
	private BulkOperator bulkOperator;
	private GetOperator operator;
	private SearchOperator searchOperator;
	private SearchScrollOperator searchScrollOperator;
	private ClearScrollOperator clearScrollOperator;

	public HElasticsearchTemplate() {
		executor = new HDefaultExecutor();
		createIndexOperator = new CreateIndexOperator();
		bulkOperator = new BulkOperator();
		operator = new GetOperator();
		searchOperator = new SearchOperator();
		searchScrollOperator = new SearchScrollOperator();
		clearScrollOperator = new ClearScrollOperator();
	}

	public HExecutor getExecutor() {
		return executor;
	}

	public void setExecutor(HExecutor executor) {
		this.executor = executor;
	}

	public <E, R, S> S exec(HOperator<E, R, S> operator, R request) {
		return executor.exec(operator, request);
	}

	public <R, S> S execHighLevel(HighLevelOperator<R, S> operator, R request) {
		return executor.exec(operator, request);
	}

	public class CreateIndexOperator extends HighLevelOperator<CreateIndexRequest, CreateIndexResponse> {

		@Override
		public CreateIndexResponse operator(RestHighLevelClient client, CreateIndexRequest request) throws IOException {
			return client.indices().create(request, getRequestOptions());
		}
	}

	public class BulkOperator extends HighLevelOperator<BulkRequest, BulkResponse> {

		@Override
		public BulkResponse operator(RestHighLevelClient client, BulkRequest request) throws IOException {
			return client.bulk(request, getRequestOptions());
		}
	}

	public class GetOperator extends HighLevelOperator<GetRequest, GetResponse> {

		@Override
		public GetResponse operator(RestHighLevelClient client, GetRequest request) throws IOException {
			return client.get(request, getRequestOptions());
		}
	}

	public class SearchOperator extends HighLevelOperator<SearchRequest, SearchResponse> {

		@Override
		public SearchResponse operator(RestHighLevelClient client, SearchRequest request) throws IOException {
			return client.search(request, getRequestOptions());
		}
	}

	public class SearchScrollOperator extends HighLevelOperator<SearchScrollRequest, SearchResponse> {

		@Override
		public SearchResponse operator(RestHighLevelClient client, SearchScrollRequest request) throws IOException {
			return client.scroll(request, getRequestOptions());
		}
	}

	public class ClearScrollOperator extends HighLevelOperator<ClearScrollRequest, ClearScrollResponse> {

		@Override
		public ClearScrollResponse operator(RestHighLevelClient client, ClearScrollRequest request) throws IOException {
			return client.clearScroll(request, getRequestOptions());
		}
	}

	public CreateIndexResponse createIndex(String index, int shards, int replicas) {
		return createIndex(index, shards, replicas, null);
	}

	public CreateIndexResponse createIndex(String index, int shards, int replicas, Map<String, ?> mapping) {
		CreateIndexRequest request = new CreateIndexRequest(index);
		request.settings(
				Settings.builder().put("index.number_of_shards", shards).put("index.number_of_replicas", replicas));
		if (mapping != null) {
			request.mapping(mapping);
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
			request.add(new IndexRequest(index).id(getId(d)).source(JSON.toJSONString(d), XContentType.JSON));
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
			request.add(new UpdateRequest(index, getId(d)).doc(JSON.toJSONString(d), XContentType.JSON));
		});
		return exec(bulkOperator, request);
	}

	public SearchResponse search(SearchSourceBuilder searchSourceBuilder, String... indices) {
		return search(searchSourceBuilder, null, indices);
	}

	public <T> List<T> search(SearchSourceBuilder searchSourceBuilder, String[] indices, Class<T> clazz) {
		Objects.requireNonNull(searchSourceBuilder, "searchSourceBuilder must not be null");
		Objects.requireNonNull(indices, "indices must not be null");
		Objects.requireNonNull(clazz, "clazz must not be null");

		SearchResponse response = search(searchSourceBuilder, null, indices);
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
		if (keepAlive != null) {
			request.scroll(keepAlive);
		}
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

	private <T> String getId(T source) {
		try {
			for (Field f : source.getClass().getDeclaredFields()) {
				if (f.isAnnotationPresent(ElasticsearchId.class)) {
					f.setAccessible(true);
					if (f.get(source) != null) {
						return f.get(source).toString();
					}
				}
			}
		} catch (Exception e) {
		}
		throw new RuntimeException("No @ElasticsearchId field was found on class");
	}

	public <T> HPageResult<T> searchScrollDeepPaging(SearchSourceBuilder searchSourceBuilder, int pageNumber,
			int pageSize, String index, String keepAlive, Class<T> clazz) {
		return searchScrollDeepPaging(searchSourceBuilder, pageNumber, pageSize, new String[] { index }, keepAlive,
				clazz);
	}

	public <T> HPageResult<T> searchScrollDeepPaging(SearchSourceBuilder searchSourceBuilder, final int pageNumber,
			final int pageSize, final String[] indices, final String keepAlive, Class<T> clazz) {

		Objects.requireNonNull(searchSourceBuilder, "searchSourceBuilder must not be null");
		Objects.requireNonNull(indices, "indices must not be null");
		Objects.requireNonNull(clazz, "clazz must not be null");

		searchSourceBuilder.from(0).size(pageSize);// scroll from=0
		SearchResponse response = search(searchSourceBuilder, keepAlive, indices);

		long total = response.getHits().getTotalHits().value;
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
			}
			response = searchScroll(response.getScrollId(), keepAlive);
		}
		clearScroll(response.getScrollId());

		return new HPageResult<T>().setList(tss).setPageNumber(pageNumber).setPageSize(pageSize).setTotal(total);
	}

}