/**
 * FileName: RetryExecutor
 * Author:   tumq
 * Date:     2018/12/14 16:00
 * Description: 重试执行器
 */
package net.dgg.framework.tac.elasticsearch.core.executor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.dgg.framework.tac.elasticsearch.core.operator.DggIOperator;
import net.dgg.framework.tac.elasticsearch.exception.DggEsException;

import java.io.IOException;

/**
 * 〈一句话功能简述〉<br> 
 * 〈重试执行器〉
 *
 * @author tumq
 * @create 2018/12/14
 */
public class DggRetryExecutor implements DggIExector{
    private final static Logger logger = LoggerFactory.getLogger(DggRetryExecutor.class);
    private boolean monitor;
    private DggExecMonitor execMonitor;
    private int retryNum;

    public DggRetryExecutor(int retryNum) {
        this.retryNum = retryNum;
    }

    public int getRetryNum() {
        return retryNum;
    }

    public void setRetryNum(int retryNum) {
        this.retryNum = retryNum;
    }

    @Override
    public void setMonitor(boolean monitor) {
        this.monitor = monitor;
    }

    @Override
    public boolean isMonitor() {
        return monitor;
    }

    @Override
    public <E,R,S> S exec(DggIOperator<E,R,S> operator,R request) throws DggEsException {
        if(logger.isDebugEnabled() && monitor) {
            execMonitor = new DggExecMonitor();
            execMonitor.setStartTime(System.currentTimeMillis());
        }
        try{
            for(int count = retryNum;count>=0;--count) {
                try {
                    return operator.operator(operator.getRestClient(), request);
                }catch(IOException io){
                	Thread.sleep(1000L);
                }
            }
            throw new DggEsException("连接异常");
        }catch(Exception ioe) {//若超时，进行重试操作
            throw new DggEsException("发现异常：" + ioe.getMessage(), ioe);
        }finally {
            if(logger.isDebugEnabled() && monitor) {
                execMonitor.setEndTime(System.currentTimeMillis());
                logger.debug("本次耗时：" + execMonitor.getSpendTime() + "毫秒");
            }
        }
    }
}