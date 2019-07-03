
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

import com.alibaba.fastjson.JSONObject;
import com.heanbian.block.elasticsearch.client.annotation.ElasticsearchId;
import com.heanbian.block.elasticsearch.client.executor.HExecutor;
import com.heanbian.block.elasticsearch.client.executor.HDefaultExecutor;
import com.heanbian.block.elasticsearch.client.operator.HOperator;
import com.heanbian.block.elasticsearch.client.operator.HighLevelOperator;
import com.heanbian.block.elasticsearch.client.page.HPage;
import com.heanbian.block.elasticsearch.client.page.HPageResult;
import com.heanbian.block.elasticsearch.client.page.HPaginationCondtion;
import com.heanbian.block.elasticsearch.client.page.HPaginationCondtionEntity;

@Component
public class HElasticsearchTemplate {

	private HExecutor executor;
	private CreateIndexOperator createIndexOperator;
	private BulkDocumentOperator bulkDocumentOperator;
	private GetDocumentOperator documentOperator;
	private SearchDocumentOperator searchDocumentOperator;
	private SearchScrollDocumentOperator searchScrollDocumentOperator;

	public HElasticsearchTemplate() {
		executor = new HDefaultExecutor();
		createIndexOperator = new CreateIndexOperator();
		bulkDocumentOperator = new BulkDocumentOperator();
		documentOperator = new GetDocumentOperator();
		searchDocumentOperator = new SearchDocumentOperator();
		searchScrollDocumentOperator = new SearchScrollDocumentOperator();
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

	public <R, S> S execHighLevel(HighLevelOperator<R, S> opertor, R request) {
		return executor.exec(opertor, request);
	}

	public class CreateIndexOperator extends HighLevelOperator<CreateIndexRequest, CreateIndexResponse> {

		@Override
		public CreateIndexResponse operator(RestHighLevelClient client, CreateIndexRequest request) throws IOException {
			return client.indices().create(request, getRequestOptions());
		}
	}

	public class BulkDocumentOperator extends HighLevelOperator<BulkRequest, BulkResponse> {

		@Override
		public BulkResponse operator(RestHighLevelClient client, BulkRequest request) throws IOException {
			return client.bulk(request, getRequestOptions());
		}
	}

	public class GetDocumentOperator extends HighLevelOperator<GetRequest, GetResponse> {

		@Override
		public GetResponse operator(RestHighLevelClient client, GetRequest request) throws IOException {
			return client.get(request, getRequestOptions());
		}
	}

	public class SearchDocumentOperator extends HighLevelOperator<SearchRequest, SearchResponse> {

		@Override
		public SearchResponse operator(RestHighLevelClient client, SearchRequest request) throws IOException {
			return client.search(request, getRequestOptions());
		}
	}

	public class SearchScrollDocumentOperator extends HighLevelOperator<SearchScrollRequest, SearchResponse> {

		@Override
		public SearchResponse operator(RestHighLevelClient client, SearchScrollRequest request) throws IOException {
			return client.scroll(request, getRequestOptions());
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

	public <T> BulkResponse bulkInsert(String index, T[] source) {
		return bulkInsert(index, Arrays.asList(source));
	}

	public <T> BulkResponse bulkInsert(String index, List<T> sources) {
		Objects.requireNonNull(index, "index must not be null");
		Objects.requireNonNull(sources, "sources must not be null");

		BulkRequest request = new BulkRequest();
		sources.forEach(d -> {
			request.add(new IndexRequest(index).id(getElasticsearchId(d)).source(JSONObject.toJSONString(d),
					XContentType.JSON));
		});
		return exec(bulkDocumentOperator, request);
	}

	public BulkResponse bulkDelete(String index, String id) {
		return bulkDelete(index, Arrays.asList(id));
	}

	public BulkResponse bulkDelete(String index, String[] id) {
		return bulkDelete(index, Arrays.asList(id));
	}

	public BulkResponse bulkDelete(String index, List<String> ids) {
		Objects.requireNonNull(ids, "ids must not be null");

		BulkRequest request = new BulkRequest();
		ids.forEach(d -> {
			request.add(new DeleteRequest(index, d));
		});
		return exec(bulkDocumentOperator, request);
	}

	public GetResponse findById(String index, String id) {
		return exec(documentOperator, new GetRequest(index, id));
	}

	public <T> T findById(String index, String id, Class<T> clazz) {
		GetResponse response = findById(index, id);
		return JSONObject.parseObject(response.getSourceAsString(), clazz);
	}

	public <T> BulkResponse bulkUpdate(String index, T source) {
		return bulkUpdate(index, Arrays.asList(source));
	}

	public <T> BulkResponse bulkUpdate(String index, T[] source) {
		return bulkUpdate(index, Arrays.asList(source));
	}

	public <T> BulkResponse bulkUpdate(String index, List<T> sources) {
		Objects.requireNonNull(index, "index must not be null");
		Objects.requireNonNull(sources, "sources must not be null");

		BulkRequest request = new BulkRequest();
		sources.forEach(d -> {
			request.add(
					new UpdateRequest(index, getElasticsearchId(d)).doc(JSONObject.toJSONString(d), XContentType.JSON));
		});
		return exec(this.bulkDocumentOperator, request);
	}

	public SearchResponse search(SearchSourceBuilder sourceBuilder, String... indices) {
		return search(sourceBuilder, null, indices);
	}

	public <T> List<T> search(SearchSourceBuilder sourceBuilder, String[] indices, Class<T> clazz) {
		SearchResponse response = search(sourceBuilder, null, indices);
		List<T> res = new ArrayList<>();
		SearchHit[] hits = response.getHits().getHits();
		for (SearchHit h : hits) {
			res.add(JSONObject.parseObject(h.getSourceAsString(), clazz));
		}
		return res;
	}

	public SearchResponse search(SearchSourceBuilder sourceBuilder, String keepAlive, String[] indices) {
		SearchRequest request = new SearchRequest(indices);
		request.source(sourceBuilder);
		if (keepAlive != null) {
			request.scroll(keepAlive);
		}
		return exec(searchDocumentOperator, request);
	}

	public SearchResponse searchScroll(String scrollId) {
		return searchScroll(scrollId, null);
	}

	public SearchResponse searchScroll(String scrollId, String keepAlive) {
		SearchScrollRequest request = new SearchScrollRequest(scrollId);
		if (keepAlive != null) {
			request.scroll(keepAlive);
		}
		return exec(searchScrollDocumentOperator, request);
	}

	public <T extends HPage> HPageResult<T> searchDeepPaging(HPaginationCondtion condtion, Class<T> clazz) {
		for (;;) {
			HPaginationCondtionEntity entity = condtion.calcPaginationCondtionEntity();

			if (entity.getQueryFromValue() >= condtion.getMaxQueryNum()) {
				condtion = condtion.nextQueryCondion(entity.getSortOrder());
				continue;
			}
			if (entity.getQueryPageNo() != condtion.getInputPageNo()) {
				condtion.getPagination().searchHits(this, entity, clazz);
				condtion = condtion.getPaginationCondtion();
				continue;
			}

			return condtion.getPagination().search(this, entity, clazz);
		}
	}

	private <T> String getElasticsearchId(T source) {
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
		throw new RuntimeException("No @ElasticsearchId field found");
	}

}