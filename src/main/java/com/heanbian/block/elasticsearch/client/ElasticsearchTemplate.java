package com.heanbian.block.elasticsearch.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.explain.ExplainRequest;
import org.elasticsearch.action.explain.ExplainResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Cancellable;
import org.elasticsearch.client.GetAliasesResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.DeleteAliasRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.InitializingBean;

import com.alibaba.fastjson.JSON;
import com.heanbian.block.elasticsearch.client.executor.DefaultExecutorImpl;
import com.heanbian.block.elasticsearch.client.executor.Executor;
import com.heanbian.block.elasticsearch.client.operator.Operator;
import com.heanbian.block.elasticsearch.client.page.Page;

/**
 * 
 * @author heanbian
 *
 */
public class ElasticsearchTemplate implements InitializingBean {

	private AliasesOperator aliasesOperator = new AliasesOperator();
	private BulkOperator bulkOperator = new BulkOperator();
	private BulkAsyncOperator bulkAsyncOperator = new BulkAsyncOperator();
	private CreateIndexOperator createIndexOperator = new CreateIndexOperator();
	private ClearScrollOperator clearScrollOperator = new ClearScrollOperator();
	private CountRequestOperator countRequestOperator = new CountRequestOperator();
	private DeleteIndexOperator deleteIndexOperator = new DeleteIndexOperator();
	private DeleteByQueryRequestOperator deleteByQueryRequestOperator = new DeleteByQueryRequestOperator();
	private DeleteAliasOperator deleteAliasOperator = new DeleteAliasOperator();
	private Executor executor = new DefaultExecutorImpl();
	private ExplainOperator explainOperator = new ExplainOperator();
	private ExistsRequestOperator existsRequestOperator = new ExistsRequestOperator();
	private GetOperator operator = new GetOperator();
	private IndicesExistsOperator indicesExistsOperator = new IndicesExistsOperator();
	private SearchOperator searchOperator = new SearchOperator();
	private SearchScrollOperator searchScrollOperator = new SearchScrollOperator();
	private UpdateRequestOperator updateRequestOperator = new UpdateRequestOperator();
	private UpdateByQueryRequestOperator updateByQueryRequestOperator = new UpdateByQueryRequestOperator();

	private RestHighLevelClient client;

	public ElasticsearchTemplate(RestHighLevelClient client) {
		this.client = client;
	}

	public <R, S> S exec(Operator<R, S> operator, R request) {
		return executor.exec(operator, request);
	}

	public class CountRequestOperator implements Operator<CountRequest, CountResponse> {

		@Override
		public CountResponse operator(CountRequest request) throws IOException {
			return client.count(request, RequestOptions.DEFAULT);
		}
	}

	public class UpdateRequestOperator implements Operator<UpdateRequest, UpdateResponse> {

		@Override
		public UpdateResponse operator(UpdateRequest request) throws IOException {
			return client.update(request, RequestOptions.DEFAULT);
		}
	}

	public class ExistsRequestOperator implements Operator<GetRequest, Boolean> {

		@Override
		public Boolean operator(GetRequest request) throws IOException {
			return client.exists(request, RequestOptions.DEFAULT);
		}
	}

	public class DeleteByQueryRequestOperator implements Operator<DeleteByQueryRequest, BulkByScrollResponse> {

		@Override
		public BulkByScrollResponse operator(DeleteByQueryRequest request) throws IOException {
			return client.deleteByQuery(request, RequestOptions.DEFAULT);
		}
	}

	public class UpdateByQueryRequestOperator implements Operator<UpdateByQueryRequest, BulkByScrollResponse> {

		@Override
		public BulkByScrollResponse operator(UpdateByQueryRequest request) throws IOException {
			return client.updateByQuery(request, RequestOptions.DEFAULT);
		}
	}

	public class CreateIndexOperator implements Operator<CreateIndexRequest, CreateIndexResponse> {

		@Override
		public CreateIndexResponse operator(CreateIndexRequest request) throws IOException {
			return client.indices().create(request, RequestOptions.DEFAULT);
		}
	}

	public class DeleteIndexOperator implements Operator<DeleteIndexRequest, AcknowledgedResponse> {

		@Override
		public AcknowledgedResponse operator(DeleteIndexRequest request) throws IOException {
			return client.indices().delete(request, RequestOptions.DEFAULT);
		}
	}

	public class DeleteAliasOperator
			implements Operator<DeleteAliasRequest, org.elasticsearch.client.core.AcknowledgedResponse> {

		@Override
		public org.elasticsearch.client.core.AcknowledgedResponse operator(DeleteAliasRequest request)
				throws IOException {
			return client.indices().deleteAlias(request, RequestOptions.DEFAULT);
		}
	}

	public class BulkOperator implements Operator<BulkRequest, BulkResponse> {

		@Override
		public BulkResponse operator(BulkRequest request) throws IOException {
			return client.bulk(request, RequestOptions.DEFAULT);
		}
	}

	public class BulkAsyncOperator implements Operator<BulkRequest, Cancellable> {

		@Override
		public Cancellable operator(BulkRequest request) throws IOException {
			return client.bulkAsync(request, RequestOptions.DEFAULT, new ActionListener<BulkResponse>() {

				@Override
				public void onResponse(BulkResponse response) {
				}

				@Override
				public void onFailure(Exception e) {
				}
			});
		}
	}

	public class ExplainOperator implements Operator<ExplainRequest, ExplainResponse> {

		@Override
		public ExplainResponse operator(ExplainRequest request) throws IOException {
			return client.explain(request, RequestOptions.DEFAULT);
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

	public <T extends ElasticsearchId> BulkResponse bulkInsert(String index, T source) {
		return bulkInsert(index, Arrays.asList(source));
	}

	public <T extends ElasticsearchId> BulkResponse bulkInsert(String index, List<T> sources) {
		Objects.requireNonNull(index, "index must not be null");
		Objects.requireNonNull(sources, "sources must not be null");

		BulkRequest request = new BulkRequest();
		sources.forEach(d -> {
			request.add(new IndexRequest(index).id(eId(d)).source(JSON.toJSONString(d), XContentType.JSON));
		});
		return exec(bulkOperator, request);
	}

	public <T extends ElasticsearchId> Cancellable bulkInsertAsync(String index, T source) {
		return bulkInsertAsync(index, Arrays.asList(source));
	}

	public <T extends ElasticsearchId> Cancellable bulkInsertAsync(String index, List<T> sources) {
		Objects.requireNonNull(index, "index must not be null");
		Objects.requireNonNull(sources, "sources must not be null");

		BulkRequest request = new BulkRequest();
		sources.forEach(d -> {
			request.add(new IndexRequest(index).id(eId(d)).source(JSON.toJSONString(d), XContentType.JSON));
		});
		return exec(bulkAsyncOperator, request);
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

	public Cancellable bulkDeleteAsync(String index, String... ids) {
		return bulkDeleteAsync(index, Arrays.asList(ids));
	}

	public Cancellable bulkDeleteAsync(String index, List<String> ids) {
		Objects.requireNonNull(index, "index must not be null");
		Objects.requireNonNull(ids, "ids must not be null");

		BulkRequest request = new BulkRequest();
		ids.forEach(id -> {
			request.add(new DeleteRequest(index, id));
		});
		return exec(bulkAsyncOperator, request);
	}

	public GetResponse findById(String index, String id) {
		Objects.requireNonNull(index, "index must not be null");
		Objects.requireNonNull(id, "id must not be null");

		return exec(operator, new GetRequest(index, id));
	}

	public <T extends ElasticsearchId> T findById(String index, String id, Class<T> clazz) {
		Objects.requireNonNull(clazz, "clazz must not be null");

		GetResponse response = findById(index, id);
		return JSON.parseObject(response.getSourceAsString(), clazz);
	}

	public <T extends ElasticsearchId> BulkResponse bulkUpdate(String index, T source) {
		return bulkUpdate(index, Arrays.asList(source));
	}

	public <T extends ElasticsearchId> BulkResponse bulkUpdate(String index, List<T> sources) {
		Objects.requireNonNull(index, "index must not be null");
		Objects.requireNonNull(sources, "sources must not be null");

		BulkRequest request = new BulkRequest();
		sources.forEach(d -> {
			request.add(new UpdateRequest(index, eId(d)).doc(JSON.toJSONString(d), XContentType.JSON));
		});
		return exec(bulkOperator, request);
	}

	public <T extends ElasticsearchId> Cancellable bulkUpdateAsync(String index, T source) {
		return bulkUpdateAsync(index, Arrays.asList(source));
	}

	public <T extends ElasticsearchId> Cancellable bulkUpdateAsync(String index, List<T> sources) {
		Objects.requireNonNull(index, "index must not be null");
		Objects.requireNonNull(sources, "sources must not be null");

		BulkRequest request = new BulkRequest();
		sources.forEach(d -> {
			request.add(new UpdateRequest(index, eId(d)).doc(JSON.toJSONString(d), XContentType.JSON));
		});
		return exec(bulkAsyncOperator, request);
	}

	public SearchResponse search(SearchSourceBuilder searchSourceBuilder, String... indices) {
		return search(searchSourceBuilder, "1m", indices);
	}

	public <T extends ElasticsearchId> List<T> search(SearchSourceBuilder searchSourceBuilder, String[] indices,
			Class<T> clazz) {
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

	private <T extends ElasticsearchId> String eId(T source) {
		return Objects.requireNonNull(source.getElasticsearchId(), "ElasticsearchId must not be null");
	}

	public <T extends ElasticsearchId> Page<T> searchScrollDeepPaging(SearchSourceBuilder searchSourceBuilder,
			int pageNumber, int pageSize, String index, String keepAlive, Class<T> clazz) {
		return searchScrollDeepPaging(searchSourceBuilder, pageNumber, pageSize, new String[] { index }, keepAlive,
				clazz);
	}

	public <T extends ElasticsearchId> Page<T> searchScrollDeepPaging(SearchSourceBuilder searchSourceBuilder,
			final int pageNumber, final int pageSize, final String[] indices, final String keepAlive, Class<T> clazz) {

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

		return new Page<T>().setList(tss).setPageNumber(pageNumber).setPageSize(pageSize).setTotal(total);
	}

	public BulkByScrollResponse deleteByQuery(QueryBuilder query, String... indices) {
		DeleteByQueryRequest request = new DeleteByQueryRequest(indices);
		request.setConflicts("proceed");
		request.setQuery(query);
		request.setBatchSize(1000);
		request.setSlices(2);
		request.setScroll(TimeValue.timeValueMinutes(10));
		request.setTimeout(TimeValue.timeValueMinutes(2));
		request.setRefresh(true);
		return deleteByQuery(request);
	}

	public BulkByScrollResponse deleteByQuery(DeleteByQueryRequest request) {
		Objects.requireNonNull(request, "request must not be null");
		return exec(deleteByQueryRequestOperator, request);
	}

	public BulkByScrollResponse updateByQuery(QueryBuilder query, String... indices) {
		UpdateByQueryRequest request = new UpdateByQueryRequest(indices);
		request.setConflicts("proceed");
		request.setQuery(query);
		request.setBatchSize(1000);
		request.setSlices(2);
		request.setScroll(TimeValue.timeValueMinutes(10));
		request.setTimeout(TimeValue.timeValueMinutes(2));
		request.setRefresh(true);
		return updateByQuery(request);
	}

	public BulkByScrollResponse updateByQuery(UpdateByQueryRequest request) {
		Objects.requireNonNull(request, "request must not be null");
		return exec(updateByQueryRequestOperator, request);
	}

	public CountResponse count(QueryBuilder query, String... indices) {
		return count(new CountRequest(indices, query));
	}

	public CountResponse count(CountRequest request) {
		Objects.requireNonNull(request, "request must not be null");
		return exec(countRequestOperator, request);
	}

	public RestHighLevelClient client() {
		return client;
	}

	public AcknowledgedResponse deleteIndex(String... indices) {
		return deleteIndex(new DeleteIndexRequest(indices));
	}

	public AcknowledgedResponse deleteIndex(DeleteIndexRequest request) {
		Objects.requireNonNull(request, "request must not be null");
		return exec(deleteIndexOperator, request);
	}

	public ExplainResponse explain(ExplainRequest request) {
		Objects.requireNonNull(request, "request must not be null");
		return exec(explainOperator, request);
	}

	public org.elasticsearch.client.core.AcknowledgedResponse deleteAlias(DeleteAliasRequest request) {
		Objects.requireNonNull(request, "request must not be null");
		return exec(deleteAliasOperator, request);
	}

	public boolean exists(String index, String id) {
		return exists(new GetRequest(index, id));
	}

	public boolean exists(GetRequest request) {
		Objects.requireNonNull(request, "request must not be null");
		return exec(existsRequestOperator, request);
	}

	public UpdateResponse update(UpdateRequest request) {
		Objects.requireNonNull(request, "request must not be null");
		return exec(updateRequestOperator, request);
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		Objects.requireNonNull(client, "client must not be null");
	}
}