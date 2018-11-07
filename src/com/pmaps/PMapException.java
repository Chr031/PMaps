package com.pmaps;

public class PMapException extends RuntimeException {

	private static final long serialVersionUID = -4254205940953992958L;

	public PMapException(String message, Throwable e) {
		super(message, e);

	}

	public PMapException(String message) {
		super(message);
	}

}
