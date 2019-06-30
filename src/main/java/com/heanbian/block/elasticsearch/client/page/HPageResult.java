package com.heanbian.block.elasticsearch.client.page;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.elasticsearch.search.SearchHit;

import com.fasterxml.jackson.annotation.JsonProperty;

public class HPageResult<I extends HPage> {

	/** 当前页号 */
	@JsonProperty("page_number")
	private int pageNumber;

	/** 每页大小 */
	@JsonProperty("page_size")
	private int pageSize;

	/** 查询总页数 */
	@JsonProperty("total_page")
	private long totalPage;

	/** 查询总数 */
	private long total;

	/** 结果列表 */
	private List<I> list;

	public HPageResult() {
	}

	public int getPageNumber() {
		return pageNumber;
	}

	public void setPageNumber(int pageNumber) {
		this.pageNumber = pageNumber;
	}

	public int getPageSize() {
		return pageSize;
	}

	public void setPageSize(int pageSize) {
		this.pageSize = pageSize;
	}

	public long getTotal() {
		return total;
	}

	public void setTotal(long total) {
		this.total = total;
	}

	public long getTotalPage() {
		// 总页数 = （总记录数 + 每页数据大小 - 1） / 每页数据大小
		return (total + pageSize - 1) / pageSize;
	}

	public List<I> getList() {
		return list;
	}

	public void setList(List<I> list) {
		this.list = list;
	}

	public void setList(SearchHit[] hits, Class<I> clazz, boolean reverse) {
		list = new ArrayList<>(hits.length);
		for (int i = 0; i < hits.length; i++) {
			Map<String, Object> source = hits[i].getSourceAsMap();
			try {
				I model = clazz.newInstance();
				List<Field> fieldList = new ArrayList<>();
				Class<?> tempClass = model.getClass();
				while (tempClass != null) {
					fieldList.addAll(Arrays.asList(tempClass.getDeclaredFields()));
					tempClass = tempClass.getSuperclass();
				}
				for (Field field : fieldList) {
					field.setAccessible(true);
					// Long类型专门处理
					if (source.get(field.getName()) != null
							&& "class java.lang.Long".equals(field.getType().toString())) {
						field.set(model, (Long.parseLong(source.get(field.getName()).toString())));
					} else {
						field.set(model, source.get(field.getName()));
					}
				}
				list.add(model);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		if (reverse) {
			Collections.reverse(list);
		}
	}
}