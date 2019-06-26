/**
 * FileName: PagenationIdPair
 * Author:   tumq
 * Date:     2018/12/19 15:17
 * Description: 存储页面起始ID与截止ID
 */
package net.dgg.framework.tac.elasticsearch.core.page;

import java.io.Serializable;

/**
 * 〈一句话功能简述〉<br> 
 * 〈存储页面起始ID与截止ID〉
 *
 * @author tumq
 * @create 2018/12/19
 */
@SuppressWarnings("serial")
public class DggPagenationIdPair implements Serializable {

    private long smallId;
    private long bigId;


    public DggPagenationIdPair(long smallId, long bigId) {
        this.smallId = smallId;
        this.bigId = bigId;
    }

    public long getSmallId() {
        return smallId;
    }

    public void setSmallId(long smallId) {
        this.smallId = smallId;
    }

    public long getBigId() {
        return bigId;
    }

    public void setBigId(long bigId) {
        this.bigId = bigId;
    }
}