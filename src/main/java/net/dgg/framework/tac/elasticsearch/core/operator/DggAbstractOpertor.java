/**
 * FileName: AbstractOpertor
 * Author:   tumq
 * Date:     2018/12/17 12:22
 * Description: 抽象操作
 */
package net.dgg.framework.tac.elasticsearch.core.operator;

import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClientBuilder;

/**
 * 〈一句话功能简述〉<br> 
 * 〈抽象操作〉
 *
 * @author tumq
 * @create 2018/12/17
 */
public abstract class DggAbstractOpertor<E,R,S> implements DggIOperator<E,R,S>{

    private RequestOptions requestOptions = RequestOptions.DEFAULT;
    private RestClientBuilder restClientBuilder;

    @Override
    public RequestOptions getRequestOptions() {
        return requestOptions;
    }

    @Override
    public void setRequestOptions(RequestOptions requestOptions) {
        this.requestOptions = requestOptions;
    }

    @Override
    public RestClientBuilder getBuilder() {
        if(restClientBuilder == null){
            restClientBuilder = DggESHighLevelClient.getInstance().getRestClientBuilder();
        }
        return restClientBuilder;
    }

    @Override
    public void setBuilder(RestClientBuilder restClientBuilder) {
        this.restClientBuilder = restClientBuilder;
    }
}