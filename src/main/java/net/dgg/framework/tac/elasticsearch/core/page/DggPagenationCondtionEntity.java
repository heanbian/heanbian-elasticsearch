/**
 * FileName: PagenationCondtionEntity
 * Author:   tumq
 * Date:     2018/12/19 19:58
 * Description: 条件计算返回实体
 */
package net.dgg.framework.tac.elasticsearch.core.page;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.sort.SortOrder;

/**
 * 〈一句话功能简述〉<br> 
 * 〈条件计算返回实体〉
 *
 * @author tumq
 * @create 2018/12/19
 */
public class DggPagenationCondtionEntity {
    /** 查询页号*/
    private int queryPageNo;

    private int queryPageSize;

    private int queryFromValue;

    private SortOrder sortOrder;

    private RangeQueryBuilder rangeQueryBuilder;

    /*查询条件*/
    private QueryBuilder inputBuilder;

    public DggPagenationCondtionEntity(int inputPageNo, int inputPageSize) {
        this.queryPageNo = inputPageNo;
        this.queryPageSize = inputPageSize;
        /*正常情况下都默认用id倒序查询*/
        sortOrder = SortOrder.DESC;
        queryFromValue = (inputPageNo - 1)*inputPageSize;//默认为首次查询的值
    }

    public int getQueryPageNo() {
        return queryPageNo;
    }

    public void setQueryPageNo(int queryPageNo) {
        this.queryPageNo = queryPageNo;
    }

    public int getQueryPageSize() {
        return queryPageSize;
    }

    public void setQueryPageSize(int queryPageSize) {
        this.queryPageSize = queryPageSize;
    }

    public int getQueryFromValue() {
        return queryFromValue;
    }

    public void setQueryFromValue(int queryFromValue) {
        this.queryFromValue = queryFromValue;
    }

    public SortOrder getSortOrder() {
        return sortOrder;
    }

    public void setSortOrder(SortOrder sortOrder) {
        this.sortOrder = sortOrder;
    }

    public RangeQueryBuilder getRangeQueryBuilder() {
        return rangeQueryBuilder;
    }

    public void setRangeQueryBuilder(RangeQueryBuilder rangeQueryBuilder) {
        this.rangeQueryBuilder = rangeQueryBuilder;
    }

    public QueryBuilder getInputBuilder() {
        return inputBuilder;
    }

    public void setInputBuilder(QueryBuilder inputBuilder) {
        this.inputBuilder = inputBuilder;
    }
}