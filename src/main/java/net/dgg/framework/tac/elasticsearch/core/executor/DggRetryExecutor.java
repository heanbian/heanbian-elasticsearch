package net.dgg.framework.tac.elasticsearch.core.executor;

import java.io.IOException;

import net.dgg.framework.tac.elasticsearch.core.operator.DggIOperator;

public class DggRetryExecutor implements DggIExector {
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
	public <E, R, S> S exec(DggIOperator<E, R, S> operator, R request) {
		if (monitor) {
			execMonitor = new DggExecMonitor();
			execMonitor.setStartTime(System.currentTimeMillis());
		}
		try {
			for (int count = retryNum; count >= 0; --count) {
				try {
					return operator.operator(operator.getRestClient(), request);
				} catch (IOException io) {
					Thread.sleep(1000L);
				}
			}
			throw new RuntimeException("连接异常");
		} catch (Exception e) {
			throw new RuntimeException("发现异常：" + e.getMessage(), e);
		} finally {
			if (monitor) {
				execMonitor.setEndTime(System.currentTimeMillis());
			}
		}
	}
}