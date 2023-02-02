package org.rdfhdt.hdt.search.component;

/**
 * Variable in an HDT search query
 *
 * @author Antoine Willerval
 */
public interface HDTVariable extends HDTComponent {

	@Override
	default boolean isVariable() {
		return true;
	}

	@Override
	default HDTVariable asVariable() {
		return this;
	}

	/**
	 * @return the variable's name, or null if this variable is nameless
	 */
	String getName();
}
