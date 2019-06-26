package net.dgg.framework.tac.elasticsearch.core.page;

import java.io.Serializable;

@SuppressWarnings("serial")
public class HPaginationIdPair implements Serializable {

	private long smallId;
	private long bigId;

	public HPaginationIdPair(long smallId, long bigId) {
		this.smallId = smallId;
		this.bigId = bigId;
	}

	public long getSmallId() {
		return smallId;
	}

	public void setSmallId(long smallId) {
		this.smallId = smallId;
	}

	public long getBigId() {
		return bigId;
	}

	public void setBigId(long bigId) {
		this.bigId = bigId;
	}
}