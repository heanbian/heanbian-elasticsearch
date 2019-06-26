package com.heanbian.block.elasticsearch.client.page;

import java.io.Serializable;

public interface HPageCache extends Serializable {

	void saveQueryPage(int pageNo, HPaginationIdPair idPair);

	HPaginationIdPair getIdPairFromQueryPage(int pageNo);

	boolean isHaveQuery(int pageNo);

	HPageGroup getClosestFromTarget(int pageNo);

	public boolean isFirstQuery();

	void clear();
}