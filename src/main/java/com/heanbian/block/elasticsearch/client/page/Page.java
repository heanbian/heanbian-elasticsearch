package com.heanbian.block.elasticsearch.client.page;

import java.util.List;

public class Page<I> {

	/**
	 * 当前页
	 */
	private int pageNumber = 1;

	/**
	 * 每页大小
	 */
	private int pageSize = 10;

	/**
	 * 总数
	 */
	private long total;

	/**
	 * 业务数据
	 */
	private List<I> list;

	public int getPageNumber() {
		return pageNumber;
	}

	public Page<I> setPageNumber(int pageNumber) {
		this.pageNumber = pageNumber;
		return this;
	}

	public int getPageSize() {
		return pageSize;
	}

	public Page<I> setPageSize(int pageSize) {
		this.pageSize = pageSize;
		return this;
	}

	public long getTotal() {
		return total;
	}

	public Page<I> setTotal(long total) {
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

	public Page<I> setList(List<I> list) {
		this.list = list;
		return this;
	}

}