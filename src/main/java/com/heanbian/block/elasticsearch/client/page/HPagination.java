package com.heanbian.block.elasticsearch.client.page;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.util.Arrays;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;

import com.heanbian.block.elasticsearch.client.HElasticsearchTemplate;

@SuppressWarnings("serial")
public class HPagination implements Serializable {

	/** 需要搜索的索引名数组 */
	private String[] indices;

	/** 用来排序的字段名 */
	private String sortFild;

	public final static String ID = "id";

	/** 最后一页 */
	private Long lastPageNum;

	/** 查询总数 */
	private Long total;

	private HPageCache pageCache;

	/**
	 * 通过此次判断本对象条件是否被修改
	 */
	private String uniqueString;

	/**
	 * 为了记录前一次的页码
	 */
	private Long pageNum = 1L;

	public Long getPageNum() {
		return pageNum;
	}

	public void setPageNum(Long pageNum) {
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

	public static String md5Encode(String inStr) {
		MessageDigest md5 = null;
		try {
			md5 = MessageDigest.getInstance("MD5");
		} catch (Exception e) {
			e.printStackTrace();
			return "";
		}
		byte[] byteArray = null;
		try {
			byteArray = inStr.getBytes("UTF-8");
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		byte[] md5Bytes = md5.digest(byteArray);
		StringBuffer hexValue = new StringBuffer();
		for (int i = 0; i < md5Bytes.length; i++) {
			int val = md5Bytes[i] & 0xff;
			if (val < 16) {
				hexValue.append("0");
			}
			hexValue.append(Integer.toHexString(val));
		}
		return hexValue.toString();
	}

	void beforeQuery(int inputPageSize, QueryBuilder inputBuilder) {
		StringBuilder b = new StringBuilder(Arrays.toString(indices)).append(sortFild).append(inputPageSize)
				.append(inputBuilder.toString());
		String md5str = md5Encode(b.toString());
		if (uniqueString == null) {
			uniqueString = md5str;
		} else {
			if (!uniqueString.equals(md5str)) {
				// 本对象条件已被改变，清除缓存，重新利用本对象进行查询，同时赋值最新的串
				pageCache.clear();
				total = null;
				lastPageNum = null;
				uniqueString = md5str;
				pageNum = 1L;
			}
		}
	}

	public Long getLastPageNum() {
		return lastPageNum;
	}

	public void setLastPageNum(Long lastPageNum) {
		this.lastPageNum = lastPageNum;
	}

	public Long getTotal() {
		return total;
	}

	public void setTotal(Long total) {
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
		if (getSortFild() != null) {
			ssb.sort(getSortFild(), entity.getSortOrder());
		}
		ssb.query(boolQuery);

		// ES查询
		SearchResponse response = template.search(ssb, getIndices());

		// 保存总条数数据
		if (total == null && lastPageNum == null) {
			total = response.getHits().getTotalHits().value;
			lastPageNum = total % entity.getQueryPageSize() > 0 ? total / entity.getQueryPageSize() + 1
					: total / entity.getQueryPageSize();
		}
		SearchHit[] hits = response.getHits().getHits();
		// 保存查询出的记录首尾ID
		if (hits.length > 0 && !pageCache.isHaveQuery(entity.getQueryPageNo())) {
			if (entity.getSortOrder() == SortOrder.DESC) {
				pageCache.saveQueryPage(entity.getQueryPageNo(),
						new HPaginationIdPair((Long) hits[hits.length - 1].getSourceAsMap().get(ID),
								(Long) hits[0].getSourceAsMap().get(ID)));
			} else {
				pageCache.saveQueryPage(entity.getQueryPageNo(),
						new HPaginationIdPair((Long) hits[0].getSourceAsMap().get(ID),
								(Long) hits[hits.length - 1].getSourceAsMap().get(ID)));
			}
		}

		return hits;
	}

	public <T extends HPage> HPageResult<T> search(HElasticsearchTemplate template, HPaginationCondtionEntity entity,
			Class<T> clazz) {
		HPageResult<T> result = new HPageResult<>();
		result.setList(searchHits(template, entity, clazz), clazz,
				entity.getSortOrder() == SortOrder.DESC ? false : true);
		result.setPageNumber(entity.getQueryPageNo());
		result.setPageSize(entity.getQueryPageSize());
		result.setTotal(total);
		return result;
	}

	public String[] getIndices() {
		return indices;
	}

	public void setIndices(String[] indices) {
		this.indices = indices;
	}

	public String getSortFild() {
		return sortFild;
	}

	public void setSortFild(String sortFild) {
		this.sortFild = sortFild;
	}

	public String asString() {
		try (ByteArrayOutputStream bout = new ByteArrayOutputStream();
				ObjectOutputStream out = new ObjectOutputStream(bout)) {
			out.writeObject(this);
			return toHexString(bout.toByteArray());
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	public static String toString(HPagination pagenation) {
		return pagenation.asString();
	}

	public static HPagination toPagination(String pagenationString) {
		try (ObjectInputStream in = new ObjectInputStream(
				new BufferedInputStream(new ByteArrayInputStream(toByteArray(pagenationString))))) {
			return (HPagination) in.readObject();
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	public static byte[] toByteArray(String hexString) {
		if (hexString == null || hexString.isEmpty()) {
			throw new IllegalArgumentException("this hexString must not be empty");
		}
		final byte[] byteArray = new byte[hexString.length() / 2];
		int k = 0;
		for (int i = 0; i < byteArray.length; i++) {// 因为是16进制，最多只会占用4位，转换成字节需要两个16进制的字符，高位在先
			byte high = (byte) (Character.digit(hexString.charAt(k), 16) & 0xff);
			byte low = (byte) (Character.digit(hexString.charAt(k + 1), 16) & 0xff);
			byteArray[i] = (byte) (high << 4 | low);
			k += 2;
		}
		return byteArray;
	}

	/**
	 * 字节数组转成16进制表示格式的字符串
	 *
	 * @param byteArray 需要转换的字节数组
	 * @return 16进制表示格式的字符串
	 **/
	public static String toHexString(byte[] byteArray) {
		if (byteArray == null || byteArray.length < 1) {
			throw new IllegalArgumentException("this byteArray must not be null or empty");
		}

		final StringBuilder hexString = new StringBuilder();
		for (int i = 0; i < byteArray.length; i++) {
			if ((byteArray[i] & 0xff) < 0x10) {// 0~F前面不零
				hexString.append("0");
			}
			hexString.append(Integer.toHexString(0xFF & byteArray[i]));
		}
		return hexString.toString();
	}
}