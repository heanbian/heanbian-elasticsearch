/**
 * FileName: IOperator
 * Author:   tumq
 * Date:     2018/12/14 16:13
 * Description: 操作方法
 */
package net.dgg.framework.tac.elasticsearch.core.operator;

import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClientBuilder;

/**
 * 〈一句话功能简述〉<br> 
 * 〈操作方法〉
 *
 * @author tumq
 * @create 2018/12/14
 */
public interface DggIOperator<E,R,S> {

    public E getRestClient();

    public S operator(E client,R request) throws Exception;

    public RestClientBuilder getBuilder();

    public void setBuilder(RestClientBuilder builder);

    public RequestOptions getRequestOptions();

    public void setRequestOptions(RequestOptions requestOptions);
}
