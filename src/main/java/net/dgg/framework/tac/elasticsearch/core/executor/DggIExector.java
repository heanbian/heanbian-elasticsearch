package net.dgg.framework.tac.elasticsearch.core.executor;

import net.dgg.framework.tac.elasticsearch.core.operator.DggIOperator;
import net.dgg.framework.tac.elasticsearch.exception.ElasticsearchException;

public interface DggIExector {

	public <E, R, S> S exec(DggIOperator<E, R, S> operator, R request) throws ElasticsearchException;

	public boolean isMonitor();

	public void setMonitor(boolean monitor);

}
