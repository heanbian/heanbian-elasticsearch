
package com.heanbian.block.elasticsearch.client.page;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.sort.SortOrder;

public class HPaginationCondtion {

	/** 查询输入页码 */
	private final int inputPageNo;

	/** 查询输入每页大小 */
	private final int inputPageSize;

	private HPagination pagination;

	/* 查询条件 */
	private QueryBuilder inputBuilder;

	/* 每次查询最大查询记录数 */
	private int maxQueryNum = 10000;

	private HPaginationCondtion paginationCondtion;

	public HPaginationCondtion() {
		this.inputPageNo = 1;
		this.inputPageSize = 10;
	}

	public HPaginationCondtion(int inputPageNumber, int inputPageSize) {
		this.inputPageNo = inputPageNumber;
		this.inputPageSize = inputPageSize;
	}

	public HPaginationCondtion(QueryBuilder queryBuilder, int inputPageNumber, int inputPageSize) {
		this(inputPageNumber, inputPageSize);
		this.inputBuilder = queryBuilder;
	}

	public int getMaxQueryNum() {
		return maxQueryNum;
	}

	public void setMaxQueryNum(int maxQueryNum) {
		this.maxQueryNum = maxQueryNum;
	}

	public int getInputPageNo() {
		return inputPageNo;
	}

	public int getInputPageSize() {
		return inputPageSize;
	}

	public HPagination getPagination() {
		return pagination;
	}

	public void setPagination(HPagination pagination) {
		this.pagination = pagination;
	}

	public HPaginationCondtion getPaginationCondtion() {
		return paginationCondtion;
	}

	public void setPaginationCondtion(HPaginationCondtion paginationCondtion) {
		this.paginationCondtion = paginationCondtion;
	}

	public QueryBuilder getInputBuilder() {
		return inputBuilder;
	}

	public void setInputBuilder(QueryBuilder inputBuilder) {
		this.inputBuilder = inputBuilder;
	}

	public HPaginationCondtion nextQueryCondion(SortOrder sortOrder) {
		HPaginationCondtion c = pagination
				.createCondtion(sortOrder == SortOrder.DESC ? inputPageNo - maxQueryNum / inputPageSize
						: inputPageNo + maxQueryNum / inputPageSize, getInputPageSize());
		c.setPaginationCondtion(this);
		c.setPagination(pagination);
		c.setInputBuilder(this.inputBuilder);
		c.setMaxQueryNum(maxQueryNum);
		return c;
	}

	public HPaginationCondtionEntity calcPaginationCondtionEntity() {
		pagination.beforeQuery(inputPageSize, inputBuilder);
		/* 默认创建首页查询实体 */
		HPaginationCondtionEntity entity = new HPaginationCondtionEntity(inputPageNo, inputPageSize);
		entity.setInputBuilder(inputBuilder);
		if (inputPageNo == 1) {
			return entity;
		}
		HPageCache pageCache = pagination.getPageCache();
		/* 非首页查询时需设置条件实体内容 */
		if (pageCache.isHaveQuery(this.inputPageNo)) {
			/**
			 * 如果已经查询过，则直接根据内存中保存的ID进行条件查询
			 */
			HPaginationIdPair pair = pageCache.getIdPairFromQueryPage(this.inputPageNo);
			entity.setRangeQueryBuilder(
					QueryBuilders.rangeQuery(HPagination.ID).gte(pair.getSmallId()).lte(pair.getBigId()));
			entity.setQueryFromValue(0);
		} else if (pagination.getLastPageNum() != null && inputPageNo == pagination.getLastPageNum()) {
			// 当输入的页码正好为最后一页，则直接按默认查询条件的倒序查询，若默认为倒序查询，则顺序查询
			entity.setQueryPageSize(
					pagination.getTotal() % inputPageSize > 0 ? (int) (pagination.getTotal() % inputPageSize)
							: inputPageSize);
			entity.setQueryFromValue(0);
			entity.setSortOrder(SortOrder.ASC);
		} else {
			HPageGroup group = pageCache.getClosestFromTarget(this.inputPageNo);
			if (!pageCache.isFirstQuery()) {
				// 这种情况为不是第一次查询，第一次查询不需要id条件
				RangeQueryBuilder builder = QueryBuilders.rangeQuery(HPagination.ID);
				builder = pageCache.isHaveQuery(group.getEndPageNo())
						? builder.gt(pageCache.getIdPairFromQueryPage(group.getEndPageNo()).getBigId())
						: builder;
				entity.setRangeQueryBuilder(pageCache.isHaveQuery(group.getStartPageNo())
						? builder.lt(pageCache.getIdPairFromQueryPage(group.getStartPageNo()).getSmallId())
						: builder);

				if (group.onlyHasStartPageNoOfClosest()) {
					entity.setQueryFromValue((inputPageNo - group.getStartPageNo() - 1) * inputPageSize);
				} else if (group.onlyHasEndPageNoOfClosest()) {
					/* 如果输入的页面离结束页比较近 */
					entity.setQueryFromValue((group.getEndPageNo() - inputPageNo - 1) * inputPageSize);
					entity.setSortOrder(SortOrder.ASC);
				} else {
					/* 如果输入的页面离起始页比较近 */
					if (group.getEndPageNo() - inputPageNo > inputPageNo - group.getStartPageNo()) {
						entity.setQueryFromValue((inputPageNo - group.getStartPageNo() - 1) * inputPageSize);
					} else {
						/* 如果输入的页面离结束页比较近 */
						entity.setQueryFromValue((group.getEndPageNo() - inputPageNo - 1) * inputPageSize);
						entity.setSortOrder(SortOrder.ASC);
					}
				}
			}
		}
		return entity;
	}
}