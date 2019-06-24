/**
 * FileName: LocalMemoryPageCache
 * Author:   tumq
 * Date:     2018/12/19 15:41
 * Description: 通过本地内存作为深度分页缓存
 */
package net.dgg.framework.tac.elasticsearch.core.page.cache;

import java.util.*;

import net.dgg.framework.tac.elasticsearch.core.page.DggPagenationIdPair;

/**
 * 〈一句话功能简述〉<br> 
 * 〈通过本地内存作为深度分页缓存〉
 *
 * @author tumq
 * @create 2018/12/19
 */
public class DggLocalMemoryPageCache implements DggIPageCache {

    /** key:第n个10条，value:起始id对象 */
    private Map<Integer, DggPagenationIdPair> idMaps = new HashMap<>();

    /** 上述map里的顺序n列表 */
    private List<Integer> pageNoLists = new ArrayList<Integer>();

    @Override
    public void saveQueryPage(int pageNo, DggPagenationIdPair idPair) {
        if(!idMaps.containsKey(pageNo)) {
            idMaps.put(pageNo, idPair);
        }
    }

    @Override
    public DggPagenationIdPair getIdPairFromQueryPage(int pageNo) {
        return idMaps.get(pageNo);
    }

    @Override
    public boolean isHaveQuery(int pageNo) {
        return idMaps.containsKey(pageNo);
    }

    /*没有查询过，则查看前后两端离条件最近的已查询过的条件，要能查到，则需将当前开始页数与结束页数添加到List，然后通过List排序定位离目标最近*/
    @Override
    public DggPageNoGroup getClosestFromTarget(int pageNo) {

        /*每次判断需重新得到当前实际查询的页码号列表*/
        pageNoLists = new ArrayList<>(idMaps.keySet());

        //首页检查目标页面是否存在
        if(!pageNoLists.contains(pageNo)){
            pageNoLists.add(pageNo);
        }
        //排序
        Collections.sort(pageNoLists);
        //获取相邻页码位置
        int pageNumIndex = pageNoLists.indexOf(pageNo);
        int pageNumStart = -1;
        try {
            pageNumStart = pageNoLists.get(pageNumIndex-1);
        }catch(IndexOutOfBoundsException e){
            pageNumStart = pageNo;
        }

        //获取相邻页码位置
        int pageNumEnd = -1;
        try {
            pageNumEnd = pageNoLists.get(pageNumIndex+1);
        }catch(IndexOutOfBoundsException e){
            pageNumEnd = pageNo;
        }

        return new DggPageNoGroup(pageNo,pageNumStart,pageNumEnd);
    }

    @Override
    public boolean isFirstQuery() {
        return idMaps.isEmpty();
    }

    @Override
    public void clear() {
        idMaps.clear();
        pageNoLists.clear();
    }
}