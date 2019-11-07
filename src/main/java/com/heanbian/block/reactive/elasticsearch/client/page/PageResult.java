package com.heanbian.block.reactive.elasticsearch.client.page;

import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.search.SearchHit;

import com.alibaba.fastjson.JSON;

public class PageResult<I> {

	/**
	 * 当前页号
	 */
	private int pageNumber;

	/**
	 * 页大小
	 */
	private int pageSize;

	/**
	 * 总页数
	 */
	private long totalPage;

	/**
	 * 总数
	 */
	private long total;

	/**
	 * 结果集
	 */
	private List<I> list;

	public PageResult() {
	}

	public int getPageNumber() {
		return pageNumber;
	}

	public PageResult<I> setPageNumber(int pageNumber) {
		this.pageNumber = pageNumber;
		return this;
	}

	public int getPageSize() {
		return pageSize;
	}

	public PageResult<I> setPageSize(int pageSize) {
		this.pageSize = pageSize;
		return this;
	}

	public long getTotal() {
		return total;
	}

	public PageResult<I> setTotal(long total) {
		this.total = total;
		return this;
	}

	// 总页数 = （总记录数 + 每页数据大小 - 1） / 每页数据大小
	public long getTotalPage() {
		this.totalPage = (total + pageSize - 1) / pageSize;
		return totalPage;
	}

	public List<I> getList() {
		return list;
	}

	public PageResult<I> setList(List<I> list) {
		this.list = list;
		return this;
	}

	public PageResult<I> setList(SearchHit[] hits, Class<I> clazz) {
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