/**
 * FileName: IExector
 * Author:   tumq
 * Date:     2018/12/14 16:10
 * Description: es执行器接口
 */
package net.dgg.framework.tac.elasticsearch.core.executor;

import net.dgg.framework.tac.elasticsearch.core.operator.DggIOperator;
import net.dgg.framework.tac.elasticsearch.exception.DggEsException;

/**
 * 〈一句话功能简述〉<br> 
 * 〈es执行器接口〉
 *
 * @author tumq
 * @create 2018/12/14
 */
public interface DggIExector{
    public <E,R,S> S exec(DggIOperator<E,R,S> operator,R request) throws DggEsException;
    public boolean isMonitor();
    public void setMonitor(boolean monitor);
}
