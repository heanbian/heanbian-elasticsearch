/**
 * FileName: ESTemplateHighLevel
 * Author:   tumq
 * Date:     2018/12/14 15:53
 * Description: ES对外访问模板
 */
package net.dgg.framework.tac.elasticsearch;

import com.alibaba.fastjson.JSONObject;

import net.dgg.framework.tac.elasticsearch.annotation.DggEsIdentify;
import net.dgg.framework.tac.elasticsearch.core.executor.DggIExector;
import net.dgg.framework.tac.elasticsearch.core.executor.DggRetryExecutor;
import net.dgg.framework.tac.elasticsearch.core.operator.DggESHighLevelOpertor;
import net.dgg.framework.tac.elasticsearch.core.operator.DggIOperator;
import net.dgg.framework.tac.elasticsearch.core.page.DggESPageResult;
import net.dgg.framework.tac.elasticsearch.core.page.DggIPageModel;
import net.dgg.framework.tac.elasticsearch.core.page.DggPagenationCondtion;
import net.dgg.framework.tac.elasticsearch.core.page.DggPagenationCondtionEntity;
import net.dgg.framework.tac.elasticsearch.exception.DggEsException;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Field;
import java.util.*;
import java.util.stream.Collectors;

/**
 * 〈一句话功能简述〉<br> 
 * 〈ES对外访问模板〉
 *
 * @author tumq
 * @create 2018/12/14
 */
@Component
public class DggESTemplate {
    private final static Logger logger = LoggerFactory.getLogger(DggESTemplate.class);
    private DggIExector exector;
    private CreateIndexOpertor createIndexOpertor;
    private InsertDocmentOpertor insertDocmentOpertor;
    private BulkDocmentOpertor bulkDocmentOpertor;
    private GetDocmentOpertor getDocmentOpertor;
    private DeleteDocmentOpertor deleteDocmentOpertor;
    private UpdateDocmentOpertor updateDocmentOpertor;
    private SearchDocmentOpertor searchDocmentOpertor;
    private SearchScrollDocmentOpertor searchScrollDocmentOpertor;
    private AllIndexOpertor allIndexOpertor;

    public DggESTemplate(){
        exector = new DggRetryExecutor(10);//默认用重试执行器
        createIndexOpertor = new CreateIndexOpertor();
        insertDocmentOpertor = new InsertDocmentOpertor();
        bulkDocmentOpertor = new BulkDocmentOpertor();
        getDocmentOpertor = new GetDocmentOpertor();
        deleteDocmentOpertor = new DeleteDocmentOpertor();
        updateDocmentOpertor = new UpdateDocmentOpertor();
        searchDocmentOpertor = new SearchDocmentOpertor();
        searchScrollDocmentOpertor = new SearchScrollDocmentOpertor();
        allIndexOpertor = new AllIndexOpertor();
    }

    public DggIExector getExector() {
        return exector;
    }

    public void setExector(DggIExector exector) {
        this.exector = exector;
    }

    public <E,R,S> S exec(DggIOperator<E,R,S> operator,R request) throws DggEsException {
        return exector.exec(operator,request);
    }

    public <R,S> S execByHighLevel(DggESHighLevelOpertor<R,S> opertor,R request) throws DggEsException {
        return exector.exec(opertor,request);
    }

    public class CreateIndexOpertor extends DggESHighLevelOpertor<CreateIndexRequest,CreateIndexResponse>{

        @Override
        public CreateIndexResponse operator(RestHighLevelClient client,CreateIndexRequest request) throws Exception {
            return client.indices().create(request, this.getRequestOptions());
        }
    }

    public class InsertDocmentOpertor extends DggESHighLevelOpertor<IndexRequest,IndexResponse>{

        @Override
        public IndexResponse operator(RestHighLevelClient client,IndexRequest request) throws Exception {
            return client.index(request, this.getRequestOptions());
        }
    }

    public class BulkDocmentOpertor extends DggESHighLevelOpertor<BulkRequest,BulkResponse>{

        @Override
        public BulkResponse operator(RestHighLevelClient client,BulkRequest request) throws Exception {
            return client.bulk(request, this.getRequestOptions());
        }
    }

    public class GetDocmentOpertor extends DggESHighLevelOpertor<GetRequest,GetResponse>{

        @Override
        public GetResponse operator(RestHighLevelClient client,GetRequest request) throws Exception {
            return client.get(request, this.getRequestOptions());
        }
    }

    public class DeleteDocmentOpertor extends DggESHighLevelOpertor<DeleteRequest,DeleteResponse>{

        @Override
        public DeleteResponse operator(RestHighLevelClient client,DeleteRequest request) throws Exception {
            return client.delete(request, this.getRequestOptions());
        }
    }

    public class UpdateDocmentOpertor extends DggESHighLevelOpertor<UpdateRequest,UpdateResponse>{

        @Override
        public UpdateResponse operator(RestHighLevelClient client,UpdateRequest request) throws Exception {
            return client.update(request, this.getRequestOptions());
        }
    }

    public class SearchDocmentOpertor extends DggESHighLevelOpertor<SearchRequest,SearchResponse>{

        @Override
        public SearchResponse operator(RestHighLevelClient client,SearchRequest request) throws Exception {
            return client.search(request, this.getRequestOptions());
        }
    }

    public class SearchScrollDocmentOpertor extends DggESHighLevelOpertor<SearchScrollRequest,SearchResponse>{

        @Override
        public SearchResponse operator(RestHighLevelClient client,SearchScrollRequest request) throws Exception {
            return client.scroll(request, this.getRequestOptions());
        }
    }

    public class AllIndexOpertor extends DggESHighLevelOpertor<Request, Response>{

        @Override
        public Response operator(RestHighLevelClient client, Request request) throws Exception {
            return client.getLowLevelClient().performRequest(request);
        }
    }

    /**
     * 获取索引信息
     * @return Response
     * @throws DggEsException
     */
    public Set<String> getAllIndex() throws DggEsException, IOException {
        Request request = new Request(
                "GET",
                "/_aliases");
        Response response = exec(allIndexOpertor, request);
        Set<String> resSet;
        try(InputStream stream = response.getEntity().getContent()){
            String result = new BufferedReader(new InputStreamReader(stream))
                    .lines().collect(Collectors.joining("\n"));
            JSONObject jsonstr = JSONObject.parseObject(result);
            resSet = jsonstr.keySet();
        }
        return resSet;
    }

    /**
     * 根据index、shards、replicas创建一个索引
     * @param index index名
     * @param shards 分片数量
     * @param replicas 副本数量
     * @return CreateIndexResponse
     * @throws IOException
     */
    public CreateIndexResponse createIndex(String index, int shards, int replicas) throws Exception {
        if(null == index) {
            throw new DggEsException("********传入参数不能为空！");
        }
        //logger.info("index=" + index + "，shards=" + shards + "，replicas=" + replicas + "，开始执行createIndex...");
        CreateIndexRequest request = new CreateIndexRequest(index);
        request.settings(Settings.builder()
                .put("index.number_of_shards", shards)
                .put("index.number_of_replicas", replicas)
        );
        return exec(createIndexOpertor,request);
    }

    /**
     * 根据index、shards、replicas创建一个索引，且自定义mapping
     * @param index index名
     * @param shards 分片数量
     * @param replicas 副本数量
     * @param type 该索引的type
     * @param mapping 该type下的自定义mapping
     * @return CreateIndexResponse
     * @throws IOException
     */
    public CreateIndexResponse createIndex(String index, int shards, int replicas, String type, Map mapping) throws Exception {
        if(null == index) {
            throw new DggEsException("********传入参数不能为空！");
        }
        //logger.info("index=" + index + "，shards=" + shards + "，replicas=" + replicas + "，开始执行createIndex...");
        CreateIndexRequest request = new CreateIndexRequest(index);
        request.mapping(type, mapping)
                .settings(Settings.builder()
                .put("index.number_of_shards", shards)
                .put("index.number_of_replicas", replicas)
        );
        return exec(createIndexOpertor,request);
    }

    private <T> List<String> getIdByListIdentify(List<T> sources) throws Exception{
        List<String> idList = new ArrayList<>();
        if(sources.size() > 0) {
            String fieldName = null;
            Field field = null;
            loop:for(T source : sources) {
                if(fieldName == null) {
                    innerloop:for (Field s : source.getClass().getDeclaredFields())
                        if (s.isAnnotationPresent(DggEsIdentify.class)) {
                            field = s;
                            fieldName = field.getName();
                            break innerloop;
                        }
                    if(field == null)
                        break loop;
                }else
                    field = source.getClass().getDeclaredField(fieldName);
                field.setAccessible(true);
                if (null == field.get(source))
                    throw new DggEsException("********注解的id值不能为null！");
                idList.add(field.get(source).toString());
            }
        }
        return idList;
    }

    private String getIdByIdentify(Object source) throws Exception{
        for(Field field : source.getClass().getDeclaredFields()) {
            if (field.isAnnotationPresent(DggEsIdentify.class)) {
                field.setAccessible(true);
                if (null == field.get(source)) {
                    throw new DggEsException("********注解的id值不能为null！");
                }
                return field.get(source).toString();
            }
        }
        return null;
    }

    /**
     * 根据index、type插入一个docment
     * @param index index名
     * @param source doc对象（对象里必须包含EsIdentify注解）
     * @return IndexResponse
     * @throws IOException
     * @throws IllegalAccessException
     * @throws IllegalArgumentException
     */
    public <T> IndexResponse insertDocment(String index,String type,String id,T source) throws Exception {
        if(null == index || null == source) {
            throw new DggEsException("********传入参数不能为空！");
        }
        //logger.info("index=" + index + "，开始执行indexOne...");
        IndexRequest request = type == null?
                (id == null? new IndexRequest(index) : new IndexRequest(index,null,id)) :
                (id == null? new IndexRequest(index,type) : new IndexRequest(index,type,id));
        request.source(JSONObject.toJSONString(source), XContentType.JSON);
        return exec(insertDocmentOpertor,request);
    }

    /**
     * 根据index、type插入一个docment
     * @param index index名
     * @param source doc对象（对象里必须包含EsIdentify注解）
     * @return IndexResponse
     * @throws IOException
     * @throws IllegalAccessException
     * @throws IllegalArgumentException
     */
    public <T> IndexResponse insertDocment(String index,T source) throws Exception {
        String _id = getIdByIdentify(source);
        return insertDocment(index,null,_id,source);
    }



    /**
     * 根据index、type插入一个docment
     * @param index index名
     * @param type type名
     * @param source doc对象（对象里必须包含EsIdentify注解）
     * @return IndexResponse
     * @throws IOException
     * @throws IllegalAccessException
     * @throws IllegalArgumentException
     */
    public <T> IndexResponse insertDocment(String index,String type,T source) throws Exception {
        if(null == index || null == type || null == source) {
            throw new DggEsException("********传入参数不能为空！");
        }
        //logger.info("index=" + index + "，type=" + type + "，开始执行indexOne...");
        String _id = getIdByIdentify(source);
        IndexRequest request = _id==null?new IndexRequest(index, type):new IndexRequest(index, type, _id);
        request.source(JSONObject.toJSONString(source), XContentType.JSON);
        return exec(insertDocmentOpertor,request);
    }



    /**
     * 根据index、type插入一批数组对象
     * @param index index名
     * @param type type名
     * @param sources 源数组（对象里必须包含EsIdentify注解）
     * @return BulkResponse
     */
    public <T> BulkResponse bulkInsert(String index, String type, List<T> sources) throws Exception {
        if(null == index || null == type || null == sources || sources.isEmpty()) {
            throw new DggEsException("********传入参数不能为空！");
        }
        //logger.info("index=" + index + "，type=" + type + "，开始执行bulkIndex...");
        BulkRequest request = new BulkRequest();
        List<String> idList = getIdByListIdentify(sources);
        for (int i=0;i < sources.size(); i++) {
            T source = sources.get(i);
            request.add(idList.size() == 0 ? new IndexRequest(index, type).source(JSONObject.toJSONString(source), XContentType.JSON)
        : new IndexRequest(index, type, idList.get(i)).source(JSONObject.toJSONString(source), XContentType.JSON));
        }
        return exec(bulkDocmentOpertor,request);
    }

    /**
     * 根据index、type、id进行批量删除
     * @param index index名
     * @param type type名
     * @param _idList es里的id列表
     * @return DeleteResponse
     */
    public BulkResponse bulkDelete(String index, String type, List<String> _idList) throws Exception {
        if(null == index || null == type || null == _idList || _idList.isEmpty()) {
            throw new DggEsException("********传入参数不能为空！");
        }
        //logger.info("index=" + index + "，type=" + type + "，开始执行bulkDelete...");
        BulkRequest request = new BulkRequest();
        _idList.forEach(_id -> {
            request.add(new DeleteRequest (index, type, _id));
        });
        return exec(bulkDocmentOpertor,request);
    }

    /**
     * 根据index、type、_id获取一个doc对象
     * @param index index名
     * @param type type名
     * @param _id es里的id
     * @return T，找不到则返回null
     */
    public GetResponse getSingleById(String index, String type, String _id) throws Exception {
        if(null == index || null == type || null == _id ) {
            throw new DggEsException("********传入参数不能为空！");
        }
        //logger.info("index=" + index + "，type=" + type + "，_id=" + _id + "，开始执行getSingleById...");
        return exec(getDocmentOpertor,new GetRequest(index, type, _id));
    }




    /**
     * 根据index、type、_id获取一个doc对象
     * @param index index名
     * @param type type名
     * @param _id es里的id
     * @param clazz 要序列化的对象
     * @return T，找不到则返回null
     */
    public <T> T getSingleById(String index, String type, String _id, Class<T> clazz) throws Exception {
        if(null == index || null == type || null == _id || null == clazz) {
            throw new DggEsException("********传入参数不能为空！");
        }
        //logger.info("index=" + index + "，type=" + type + "，_id=" + _id + "，开始执行getSingleById...");
        GetResponse response = exec(getDocmentOpertor,new GetRequest(index, type, _id));
        if(response.isExists()) {
            String sourceAsString = response.getSourceAsString();
            return JSONObject.parseObject(sourceAsString, clazz);
        }else {
            throw new DggEsException("index not found!");
        }
    }

    /**
     * 根据index、type、_id删除一个doc对象
     * @param index index名
     * @param type type名
     * @param _id es里的id
     * @return DeleteResponse
     */
    public DeleteResponse deleteById(String index, String type, String _id) throws Exception {
        if(null == index || null == type || null == _id) {
            throw new DggEsException("********传入参数不能为空！");
        }
        //logger.info("index=" + index + "，type=" + type + "，_id=" + _id + "，开始执行deleteById...");
        return exec(deleteDocmentOpertor,new DeleteRequest(index,type,_id));
    }



    /**
     * 根据index、type、_id更新一个doc对象
     * @param index index名
     * @param type type名
     * @param _id es里的id
     * @param source 更新的文档对象
     * @return UpdateResponse
     */
    public <T> UpdateResponse updateById(String index, String type, String _id, T source) throws Exception {
        if(null == index || null == type || null == _id){
            throw new DggEsException("********传入参数不能为空！");
        }
        //logger.info("index=" + index + "，type=" + type + "，_id=" + _id + "，开始执行updateById...");
        UpdateRequest request = new UpdateRequest(index, type, _id);
        request.doc(JSONObject.toJSONString(source), XContentType.JSON);
        return exec(updateDocmentOpertor,request);
    }

    /**
     * 批量更新一组数据
     * @param index index名
     * @param type type名
     * @param sources 更新的文档对象（需要指明EsIdentify）
     * @return BulkResponse
     */
    public <T> BulkResponse bulkUpdate(String index, String type, List<T> sources) throws Exception {
        if(null == index || null == type || null == sources || sources.isEmpty()) {
            throw new DggEsException("********传入参数不能为空！");
        }
        //logger.info("index=" + index + "，type=" + type + "，开始执行bulkUpdate...");
        BulkRequest request = new BulkRequest();
        List<String> idList = getIdByListIdentify(sources);
        if(idList.size() == 0){
            throw new DggEsException("不明确的ID值，无法更新");
        }
        for (int i=0;i < sources.size(); i++) {
            T source = sources.get(i);
            request.add(new UpdateRequest(index, type, idList.get(i)).doc(JSONObject.toJSONString(source), XContentType.JSON));
        }
        return exec(this.bulkDocmentOpertor,request);
    }

    /**
     * 执行搜索
     * @param index index名
     * @param type type名
     * @param sourceBuilder 组装的SearchSourceBuilder搜索对象
     * @return SearchResponse
     */
    public SearchResponse query(String index, String type, SearchSourceBuilder sourceBuilder) throws Exception {
        return query(sourceBuilder, type, index);
    }

    /**
     * 执行搜索
     * @param sourceBuilder 组装的SearchSourceBuilder搜索对象
     * @param type type名
     * @param indices index名
     * @return SearchResponse
     */
    public SearchResponse query(SearchSourceBuilder sourceBuilder, String type, String ... indices) throws Exception {
        if(null == indices || null == type || null == sourceBuilder) {
            throw new DggEsException("********传入参数不能为空！");
        }
        SearchRequest request = new SearchRequest(indices);
        request.types(type);
        request.source(sourceBuilder);
        return exec(searchDocmentOpertor,request);
    }

    /**
     * 执行scroll搜索（第一次）
     * @param index index名
     * @param type type名
     * @param sourceBuilder 组装的SearchSourceBuilder搜索对象
     * @param keepAlive scroll有效时间，例如1m、2m等
     * @return SearchResponse
     */
    public SearchResponse queryScroll(String index, String type, SearchSourceBuilder sourceBuilder, String keepAlive) throws Exception {
        return queryScroll(type, sourceBuilder, keepAlive, index);
    }

    /**
     * 执行scroll搜索（第一次）
     * @param type type名
     * @param sourceBuilder 组装的SearchSourceBuilder搜索对象
     * @param keepAlive scroll有效时间，例如1m、2m等
     * @param indices index名
     * @return SearchResponse
     */
    public SearchResponse queryScroll(String type, SearchSourceBuilder sourceBuilder, String keepAlive, String ... indices) throws Exception {
        if(null == indices || null == type || null == sourceBuilder || null == keepAlive) {
            throw new DggEsException("********传入参数不能为空！");
        }
        //logger.info("index=" + index + "，type=" + type + "，keepAlive=" + keepAlive + "，开始执行搜索：" + sourceBuilder.toString());
        SearchRequest request = new SearchRequest(indices);
        request.types(type);
        request.source(sourceBuilder);
        request.scroll(keepAlive);
        SearchResponse response = exec(searchDocmentOpertor,request);
        return response;
    }

    /**
     * 执行scroll搜索
     * @param scrollId scrollId
     * @param keepAlive scroll有效时间，例如1m、2m等
     * @return SearchResponse
     */
    public SearchResponse queryScroll(String scrollId, String keepAlive) throws Exception {
        if(null == scrollId) {
            throw new DggEsException("********传入参数不能为空！");
        }
        //logger.info("scrollId=" + scrollId + "，keepAlive=" + keepAlive + "0，开始执行搜索");
        SearchScrollRequest request = new SearchScrollRequest(scrollId);
        request.scroll(keepAlive);
        SearchResponse response = exec(searchScrollDocmentOpertor,request);
        return response;
    }


    /**
     * 深度分页查询
     * @param condtion
     * @return
     * @throws Exception
     */
    public <T extends DggIPageModel> DggESPageResult<T> queryByDeeppaging(DggPagenationCondtion condtion, Class<T> beanCls) throws Exception{
        DggPagenationCondtion queryCondtion = condtion;
        while(true) {
            DggPagenationCondtionEntity entity = queryCondtion.calcPagenationCondtionEntity();
            /*默认跳过循环条数*/
            if (entity.getQueryFromValue() >= queryCondtion.getMaxQueryNum()) {
                queryCondtion = queryCondtion.nextQueryCondion(entity.getSortOrder());
                continue;
            }
            if (entity.getQueryPageNo() != condtion.getInputPageNo()){
                queryCondtion.getEsPagenation().queryHits(this, entity, beanCls);
                queryCondtion = queryCondtion.getPreCondtion();
                continue;
            }

            return queryCondtion.getEsPagenation().query(this, entity, beanCls);
        }
    }
}