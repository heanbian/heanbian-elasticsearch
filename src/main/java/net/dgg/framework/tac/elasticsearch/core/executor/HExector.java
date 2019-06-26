package net.dgg.framework.tac.elasticsearch.core.executor;

import net.dgg.framework.tac.elasticsearch.core.operator.HOperator;

public interface HExector {

	public <E, R, S> S exec(HOperator<E, R, S> operator, R request);

	public boolean isMonitor();

	public void setMonitor(boolean monitor);

}
