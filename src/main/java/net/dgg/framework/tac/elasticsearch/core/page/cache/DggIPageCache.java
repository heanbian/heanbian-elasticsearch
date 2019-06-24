/**
 * FileName: IPageCache
 * Author:   tumq
 * Date:     2018/12/19 15:13
 * Description: 深度分页缓存
 */
package net.dgg.framework.tac.elasticsearch.core.page.cache;

import java.io.Serializable;

import net.dgg.framework.tac.elasticsearch.core.page.DggPagenationIdPair;

/**
 * 〈一句话功能简述〉<br> 
 * 〈深度分页缓存〉
 *
 * @author tumq
 * @create 2018/12/19
 */
public interface DggIPageCache extends Serializable {

    void saveQueryPage(int pageNo, DggPagenationIdPair idPair);

    DggPagenationIdPair getIdPairFromQueryPage(int pageNo);

    boolean isHaveQuery(int pageNo);

    /*获取离目标最近的页码，获取离页面最近的开始页与结束页*/
    DggPageNoGroup getClosestFromTarget(int pageNo);

    public boolean isFirstQuery();

    void clear();
}