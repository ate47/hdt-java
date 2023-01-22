package org.rdfhdt.hdt.search;

import org.junit.Test;
import org.rdfhdt.hdt.hdt.HDTFactory;
import org.rdfhdt.hdt.search.component.HDTComponentTriple;

import java.io.IOException;

import static org.junit.Assert.assertNotNull;

public class QueryServiceTest extends AbstractQueryTest {
	@Test
	public void serviceTest() {
		assertNotNull("CORE factory wasn't found!", FACTORY);
	}
}
