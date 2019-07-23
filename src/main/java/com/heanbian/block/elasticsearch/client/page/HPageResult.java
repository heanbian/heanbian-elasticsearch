package com.heanbian.block.elasticsearch.client.page;

import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.search.SearchHit;

import com.alibaba.fastjson.JSON;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class HPageResult<I> {

	/**
	 * 当前页号
	 */
	@JsonProperty("page_number")
	private int pageNumber;

	/**
	 * 页大小
	 */
	@JsonProperty("page_size")
	private int pageSize;

	/**
	 * 总页数
	 */
	@JsonProperty("total_page")
	private long totalPage;

	/**
	 * 总数
	 */
	private long total;

	/**
	 * 结果集
	 */
	private List<I> list;

	public HPageResult() {}

	public int getPageNumber() {
		return pageNumber;
	}

	public HPageResult<I> setPageNumber(int pageNumber) {
		this.pageNumber = pageNumber;
		return this;
	}

	public int getPageSize() {
		return pageSize;
	}

	public HPageResult<I> setPageSize(int pageSize) {
		this.pageSize = pageSize;
		return this;
	}

	public long getTotal() {
		return total;
	}

	public HPageResult<I> setTotal(long total) {
		this.total = total;
		return this;
	}

	// 总页数 = （总记录数 + 每页数据大小 - 1） / 每页数据大小
	public long getTotalPage() {
		return (total + pageSize - 1) / pageSize;
	}

	public List<I> getList() {
		return list;
	}

	public HPageResult<I> setList(List<I> list) {
		this.list = list;
		return this;
	}

	public HPageResult<I> setList(SearchHit[] hits, Class<I> clazz) {
		final int len = hits.length;
		if (list == null) {
			list = new ArrayList<>(len);
		}
		for (int i = 0; i < len; i++) {
			list.add(JSON.parseObject(hits[i].getSourceAsString(), clazz));
		}
		return this;
	}

}