/**
 * FileName: ExecMonitor
 * Author:   tumq
 * Date:     2018/12/17 9:13
 * Description: 执行监控
 */
package net.dgg.framework.tac.elasticsearch.core.executor;

/**
 * 〈一句话功能简述〉<br> 
 * 〈执行监控〉
 *
 * @author tumq
 * @create 2018/12/17
 */
public class DggExecMonitor {
    private long startTime;
    private long endTime;

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    public long getSpendTime(){
        return endTime - startTime;
    }
}