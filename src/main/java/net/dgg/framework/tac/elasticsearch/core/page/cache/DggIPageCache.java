package net.dgg.framework.tac.elasticsearch.core.page.cache;

import java.io.Serializable;

import net.dgg.framework.tac.elasticsearch.core.page.DggPagenationIdPair;

public interface DggIPageCache extends Serializable {

	void saveQueryPage(int pageNo, DggPagenationIdPair idPair);

	DggPagenationIdPair getIdPairFromQueryPage(int pageNo);

	boolean isHaveQuery(int pageNo);

	DggPageNoGroup getClosestFromTarget(int pageNo);

	public boolean isFirstQuery();

	void clear();
}