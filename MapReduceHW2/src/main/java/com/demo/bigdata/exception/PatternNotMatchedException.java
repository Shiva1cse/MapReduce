package com.demo.bigdata.exception;

public class PatternNotMatchedException extends Exception {
	/**
	 * To throw the exception when the logs data is not matched.
	 */
	private static final long serialVersionUID = 5278597862394055663L;
	String message;

	public PatternNotMatchedException(String message) {
		this.message = message;
	}
	
	@Override
	public String toString() {
		return "PatternNotMatchedException [message=" + message + "]";
	}

}
