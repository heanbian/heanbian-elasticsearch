package net.dgg.framework.tac.elasticsearch.core.page.cache;

import java.io.Serializable;

import net.dgg.framework.tac.elasticsearch.core.page.HPaginationIdPair;

public interface HPageCache extends Serializable {

	void saveQueryPage(int pageNo, HPaginationIdPair idPair);

	HPaginationIdPair getIdPairFromQueryPage(int pageNo);

	boolean isHaveQuery(int pageNo);

	HPageGroup getClosestFromTarget(int pageNo);

	public boolean isFirstQuery();

	void clear();
}