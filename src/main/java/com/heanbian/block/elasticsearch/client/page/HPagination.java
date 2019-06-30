package com.heanbian.block.elasticsearch.client.page;

import java.io.Serializable;
import java.util.Arrays;

import org.apache.commons.codec.digest.DigestUtils;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;

import com.alibaba.fastjson.JSONObject;
import com.heanbian.block.elasticsearch.client.HElasticsearchTemplate;

@SuppressWarnings("serial")
public class HPagination implements Serializable {

	/** 需要搜索的索引名数组 */
	private String[] indices;

	/** 用来排序的字段名 */
	private String sortField;

	public final static String ID = "id";

	/** 最后一页 */
	private long lastPageNum;

	/** 查询总数 */
	private long total;

	private HPageCache pageCache;

	/**
	 * 通过此次判断本对象条件是否被修改
	 */
	private String uniqueString;

	/**
	 * 为了记录前一次的页码
	 */
	private long pageNum = 1L;

	public long getPageNum() {
		return pageNum;
	}

	public void setPageNum(long pageNum) {
		this.pageNum = pageNum;
	}

	public HPagination() {
		this.pageCache = new HMemoryPageCache();
	}

	public HPagination(HPageCache pageCache) {
		this.pageCache = pageCache;
	}

	public HPageCache getPageCache() {
		return pageCache;
	}

	public void setPageCache(HPageCache pageCache) {
		this.pageCache = pageCache;
	}

	void beforeQuery(int inputPageSize, QueryBuilder inputBuilder) {
		StringBuilder b = new StringBuilder(Arrays.toString(indices)).append(sortField).append(inputPageSize)
				.append(inputBuilder.toString());
		String md5str = DigestUtils.md5Hex(b.toString());
		if (uniqueString == null) {
			uniqueString = md5str;
		} else {
			if (!uniqueString.equals(md5str)) {
				// 本对象条件已被改变，清除缓存，重新利用本对象进行查询，同时赋值最新的串
				pageCache.clear();
				total = 0;
				lastPageNum = 0;
				uniqueString = md5str;
				pageNum = 1L;
			}
		}
	}

	public long getLastPageNum() {
		return lastPageNum;
	}

	public void setLastPageNum(long lastPageNum) {
		this.lastPageNum = lastPageNum;
	}

	public long getTotal() {
		return total;
	}

	public void setTotal(long total) {
		this.total = total;
	}

	public HPaginationCondtion createCondtion(int inputPageNumber, int inputPageSize) {
		HPaginationCondtion c = new HPaginationCondtion(inputPageNumber, inputPageSize);
		c.setPagination(this);
		return c;
	}

	public HPaginationCondtion createCondtion(int inputPageNumber, int inputPageSize, QueryBuilder inputBuilder) {
		HPaginationCondtion c = new HPaginationCondtion(inputPageNumber, inputPageSize);
		c.setPagination(this);
		c.setInputBuilder(inputBuilder);
		return c;
	}

	public <T extends HPage> SearchHit[] searchHits(HElasticsearchTemplate template, HPaginationCondtionEntity entity,
			Class<T> clazz) {
		SearchSourceBuilder ssb = new SearchSourceBuilder();
		BoolQueryBuilder boolQuery = new BoolQueryBuilder();
		// 添加外部查询条件
		boolQuery = entity.getInputBuilder() != null ? boolQuery.filter(entity.getInputBuilder()) : boolQuery;
		// 添加内部生成条件
		if (entity.getRangeQueryBuilder() != null) {
			boolQuery.must(entity.getRangeQueryBuilder());
		}

		ssb.from(entity.getQueryFromValue()).size(entity.getQueryPageSize());
		ssb.sort(ID, entity.getSortOrder());
		if (getSortField() != null) {
			ssb.sort(getSortField(), entity.getSortOrder());
		}
		ssb.query(boolQuery);

		SearchResponse response = template.search(ssb, getIndices());

		// 保存总条数数据
		if (total == 0 && lastPageNum == 0) {
			total = response.getHits().getTotalHits().value;
			lastPageNum = total % entity.getQueryPageSize() > 0 ? total / entity.getQueryPageSize() + 1
					: total / entity.getQueryPageSize();
		}
		SearchHit[] hits = response.getHits().getHits();
		// 保存查询出的记录首尾ID
		if (hits.length > 0 && !pageCache.isHaveQuery(entity.getQueryPageNo())) {
			SearchHit h1 = hits[hits.length - 1];
			SearchHit h2 = hits[0];
			if (entity.getSortOrder() == SortOrder.DESC) {
				pageCache.saveQueryPage(entity.getQueryPageNo(),
						new HPaginationIdPair(Long.valueOf((h1.getSourceAsMap().get(ID).toString())),
								Long.valueOf(h2.getSourceAsMap().get(ID).toString())));
			} else {
				pageCache.saveQueryPage(entity.getQueryPageNo(),
						new HPaginationIdPair(Long.valueOf(h2.getSourceAsMap().get(ID).toString()),
								Long.valueOf(h1.getSourceAsMap().get(ID).toString())));
			}
		}
		return hits;
	}

	public <T extends HPage> HPageResult<T> search(HElasticsearchTemplate t, HPaginationCondtionEntity c,
			Class<T> clazz) {
		HPageResult<T> result = new HPageResult<>();
		result.setList(searchHits(t, c, clazz), clazz);
		result.setPageNumber(c.getQueryPageNo());
		result.setPageSize(c.getQueryPageSize());
		result.setTotal(total);
		return result;
	}

	public String[] getIndices() {
		return indices;
	}

	public void setIndices(String[] indices) {
		this.indices = indices;
	}

	public String getSortField() {
		return sortField;
	}

	public void setSortField(String sortField) {
		this.sortField = sortField;
	}

	public static String toString(HPagination pagenation) {
		return JSONObject.toJSONString(pagenation);
	}

	public static HPagination toPagination(String pagenationString) {
		return JSONObject.parseObject(pagenationString, HPagination.class);
	}

}