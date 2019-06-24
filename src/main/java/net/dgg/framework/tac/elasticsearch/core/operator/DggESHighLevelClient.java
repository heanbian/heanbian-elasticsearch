/**
 * FileName: ESHighLevelClient
 * Author:   tumq
 * Date:     2018/12/17 15:01
 * Description: 高亮客户端
 */
package net.dgg.framework.tac.elasticsearch.core.operator;

import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

/**
 * 〈一句话功能简述〉<br> 
 * 〈高亮客户端〉
 *
 * @author tumq
 * @create 2018/12/17
 */
@Component
public class DggESHighLevelClient implements ApplicationContextAware {

    private RestHighLevelClient restHighLevelClient;

    @Autowired
    private RestClientBuilder restClientBuilder;

    private static DggESHighLevelClient client;

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        restHighLevelClient = new RestHighLevelClient(restClientBuilder);
        client = this;
    }

    public final static DggESHighLevelClient getInstance(){
        return client;
    }

    public RestHighLevelClient getRestHighLevelClient() {
        return restHighLevelClient;
    }

    public void setRestHighLevelClient(RestHighLevelClient restHighLevelClient) {
        this.restHighLevelClient = restHighLevelClient;
    }

    public RestClientBuilder getRestClientBuilder() {
        return restClientBuilder;
    }

    public void setRestClientBuilder(RestClientBuilder restClientBuilder) {
        this.restClientBuilder = restClientBuilder;
    }
}