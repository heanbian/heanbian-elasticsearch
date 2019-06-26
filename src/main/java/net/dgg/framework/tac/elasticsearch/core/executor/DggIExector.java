package net.dgg.framework.tac.elasticsearch.core.executor;

import net.dgg.framework.tac.elasticsearch.core.operator.DggIOperator;

public interface DggIExector {

	public <E, R, S> S exec(DggIOperator<E, R, S> operator, R request);

	public boolean isMonitor();

	public void setMonitor(boolean monitor);

}
