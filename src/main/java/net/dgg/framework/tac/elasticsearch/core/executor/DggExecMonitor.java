package net.dgg.framework.tac.elasticsearch.core.executor;

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

	public long getSpendTime() {
		return endTime - startTime;
	}
}