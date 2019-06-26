
package net.dgg.framework.tac.elasticsearch;

import java.io.IOException;
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
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.stereotype.Component;

import com.alibaba.fastjson.JSONObject;

import net.dgg.framework.tac.elasticsearch.annotation.ElasticsearchId;
import net.dgg.framework.tac.elasticsearch.core.executor.DggIExector;
import net.dgg.framework.tac.elasticsearch.core.executor.DggRetryExecutor;
import net.dgg.framework.tac.elasticsearch.core.operator.DggESHighLevelOpertor;
import net.dgg.framework.tac.elasticsearch.core.operator.DggIOperator;
import net.dgg.framework.tac.elasticsearch.core.page.DggESPageResult;
import net.dgg.framework.tac.elasticsearch.core.page.DggIPageModel;
import net.dgg.framework.tac.elasticsearch.core.page.DggPagenationCondtion;
import net.dgg.framework.tac.elasticsearch.core.page.DggPagenationCondtionEntity;

@Component
public class ElasticsearchTemplate {

	private DggIExector exector;
	private CreateIndexOpertor createIndexOpertor;
	private BulkDocmentOpertor bulkDocumentOpertor;
	private GetDocumentOpertor documentOpertor;
	private SearchDocumentOpertor searchDocumentOpertor;
	private SearchScrollDocumentOpertor searchScrollDocumentOpertor;

	public ElasticsearchTemplate() {
		exector = new DggRetryExecutor(10);
		createIndexOpertor = new CreateIndexOpertor();
		bulkDocumentOpertor = new BulkDocmentOpertor();
		documentOpertor = new GetDocumentOpertor();
		searchDocumentOpertor = new SearchDocumentOpertor();
		searchScrollDocumentOpertor = new SearchScrollDocumentOpertor();
	}

	public DggIExector getExector() {
		return exector;
	}

	public void setExector(DggIExector exector) {
		this.exector = exector;
	}

	public <E, R, S> S exec(DggIOperator<E, R, S> operator, R request) {
		return exector.exec(operator, request);
	}

	public <R, S> S execByHighLevel(DggESHighLevelOpertor<R, S> opertor, R request) {
		return exector.exec(opertor, request);
	}

	public class CreateIndexOpertor extends DggESHighLevelOpertor<CreateIndexRequest, CreateIndexResponse> {

		@Override
		public CreateIndexResponse operator(RestHighLevelClient client, CreateIndexRequest request) throws IOException {
			return client.indices().create(request, getRequestOptions());
		}
	}

	public class BulkDocmentOpertor extends DggESHighLevelOpertor<BulkRequest, BulkResponse> {

		@Override
		public BulkResponse operator(RestHighLevelClient client, BulkRequest request) throws IOException {
			return client.bulk(request, getRequestOptions());
		}
	}

	public class GetDocumentOpertor extends DggESHighLevelOpertor<GetRequest, GetResponse> {

		@Override
		public GetResponse operator(RestHighLevelClient client, GetRequest request) throws IOException {
			return client.get(request, getRequestOptions());
		}
	}

	public class SearchDocumentOpertor extends DggESHighLevelOpertor<SearchRequest, SearchResponse> {

		@Override
		public SearchResponse operator(RestHighLevelClient client, SearchRequest request) throws IOException {
			return client.search(request, getRequestOptions());
		}
	}

	public class SearchScrollDocumentOpertor extends DggESHighLevelOpertor<SearchScrollRequest, SearchResponse> {

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
		return exec(createIndexOpertor, request);
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
			ElasticsearchId id = d.getClass().getAnnotation(ElasticsearchId.class);
			request.add(
					new IndexRequest(index).id(id.toString()).source(JSONObject.toJSONString(d), XContentType.JSON));
		});
		return exec(bulkDocumentOpertor, request);
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
		return exec(bulkDocumentOpertor, request);
	}

	public GetResponse findById(String index, String id) {
		return exec(documentOpertor, new GetRequest(index, id));
	}

	public <T> T findById(String index, String id, Class<T> clazz) {
		GetResponse response = exec(documentOpertor, new GetRequest(index, id));
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
			ElasticsearchId id = d.getClass().getAnnotation(ElasticsearchId.class);
			request.add(new UpdateRequest(index, id.toString()).doc(JSONObject.toJSONString(d), XContentType.JSON));
		});
		return exec(this.bulkDocumentOpertor, request);
	}

	public SearchResponse search(SearchSourceBuilder sourceBuilder, String... indices) {
		return search(sourceBuilder, null, indices);
	}

	public SearchResponse search(SearchSourceBuilder sourceBuilder, String keepAlive, String... indices) {
		SearchRequest request = new SearchRequest(indices);
		request.source(sourceBuilder);
		if (keepAlive != null) {
			request.scroll(keepAlive);
		}
		return exec(searchDocumentOpertor, request);
	}

	public SearchResponse searchScroll(String scrollId) {
		return searchScroll(scrollId, null);
	}

	public SearchResponse searchScroll(String scrollId, String keepAlive) {
		SearchScrollRequest request = new SearchScrollRequest(scrollId);
		if (keepAlive != null) {
			request.scroll(keepAlive);
		}
		return exec(searchScrollDocumentOpertor, request);
	}

	public <T extends DggIPageModel> DggESPageResult<T> searchByDeeppaging(DggPagenationCondtion condtion,
			Class<T> clazz) {
		DggPagenationCondtion queryCondtion = condtion;
		for (;;) {
			DggPagenationCondtionEntity entity = queryCondtion.calcPagenationCondtionEntity();

			if (entity.getQueryFromValue() >= queryCondtion.getMaxQueryNum()) {
				queryCondtion = queryCondtion.nextQueryCondion(entity.getSortOrder());
				continue;
			}
			if (entity.getQueryPageNo() != condtion.getInputPageNo()) {
				queryCondtion.getEsPagenation().queryHits(this, entity, clazz);
				queryCondtion = queryCondtion.getPreCondtion();
				continue;
			}

			return queryCondtion.getEsPagenation().query(this, entity, clazz);
		}
	}
}