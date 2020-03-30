package com.heanbian.block.elasticsearch.client.page;

import java.util.List;

public class Page<I> {

	/**
	 * 当前页号
	 */
	private int pageNumber = 1;

	/**
	 * 页大小
	 */
	private int pageSize = 10;

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

	public Page() {
	}

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
		this.totalPage = (total + pageSize - 1) / pageSize;
		return totalPage;
	}

	public List<I> getList() {
		return list;
	}

	public Page<I> setList(List<I> list) {
		this.list = list;
		return this;
	}

}