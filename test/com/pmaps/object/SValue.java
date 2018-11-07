package com.pmaps.object;

import java.io.Serializable;
import java.util.Arrays;

public class SValue implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 2066089218642162596L;
	private final byte[] content;

	public SValue(byte[] content) {
		super();
		this.content = content;
	}

	public byte[] getContent() {
		return content;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Arrays.hashCode(content);
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		SValue other = (SValue) obj;
		if (!Arrays.equals(content, other.content))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "SValue [content=" + Arrays.toString(content) + "]";
	}
}