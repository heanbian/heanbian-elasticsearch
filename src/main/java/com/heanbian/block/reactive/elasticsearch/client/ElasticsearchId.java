package com.heanbian.block.reactive.elasticsearch.client;

/**
 * Class T must extend class ElasticsearchId
 * 
 * @author heanbian
 *
 */
public abstract class ElasticsearchId {

	private String eId;

	public String getEId() {
		return eId;
	}

	public void setEId(String eId) {
		this.eId = eId;
	}

}
