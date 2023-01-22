package org.rdfhdt.hdt.search;

import org.rdfhdt.hdt.hdt.HDT;

import java.util.Objects;

/**
 * Implementation of {@link HDTQueryToolFactory}
 *
 * @author Antoine Willerval
 */
public class HDTQueryToolFactoryImpl extends HDTQueryToolFactory {

	@Override
	public HDTQueryTool newGenericQueryTool(HDT hdt) {
		Objects.requireNonNull(hdt, "hdt can't be null!");
		return new SimpleQueryTool(hdt);
	}

	@Override
	public boolean hasGenericTool() {
		return true;
	}

	@Override
	public HDTQueryTool newQueryTool(HDT hdt) {
		// TODO: specific implementation working with BitmapTriples
		return null;
	}
}
