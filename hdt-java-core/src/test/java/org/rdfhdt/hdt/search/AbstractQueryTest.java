package org.rdfhdt.hdt.search;

import org.rdfhdt.hdt.hdt.HDTFactory;
import org.rdfhdt.hdt.util.io.AbstractMapMemoryTest;

import java.io.IOException;

public class AbstractQueryTest extends AbstractMapMemoryTest {
	protected static final HDTQueryToolFactoryImpl FACTORY;
	protected static final HDTQueryTool MOCK_FACTORY;

	static {
		FACTORY = (HDTQueryToolFactoryImpl)
				HDTQueryToolFactory.getFactories().stream()
						.filter(factory -> factory instanceof HDTQueryToolFactoryImpl)
						.findAny()
						.orElseThrow(() -> new AssertionError("Can't find " + HDTQueryToolFactoryImpl.class));
		try {
			MOCK_FACTORY = FACTORY.newGenericQueryTool(HDTFactory.createHDT());
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}