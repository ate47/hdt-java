package org.rdfhdt.hdt.util;

/**
 * A simple runtime exception to contain a cause
 * @author Antoine Willerval
 */
public class ContainerException extends RuntimeException {

	public ContainerException(Throwable cause) {
		super(cause);
	}
}
