package net.dgg.framework.tac.elasticsearch.exception;

@SuppressWarnings("serial")
public class ElasticsearchException extends Exception {

	public ElasticsearchException(String message) {
		super(message);
	}

	public ElasticsearchException(String message, Throwable e) {
		super(message, e);
	}
}