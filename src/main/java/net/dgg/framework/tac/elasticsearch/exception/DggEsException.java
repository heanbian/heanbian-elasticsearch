package net.dgg.framework.tac.elasticsearch.exception;

public class DggEsException extends Exception{

	/**
	 * 
	 */
	private static final long serialVersionUID = 4542506854695451067L;

	public DggEsException(String message) {
		super(message);
	}
	public DggEsException(String message, Throwable cause) {
		super(message, cause);
	}
}