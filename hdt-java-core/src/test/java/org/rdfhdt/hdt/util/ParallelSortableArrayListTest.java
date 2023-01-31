package org.rdfhdt.hdt.util;

import org.junit.Test;

import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.*;

public class ParallelSortableArrayListTest {
	@Test
	public void initTest() {
		ParallelSortableArrayList<String> test = new ParallelSortableArrayList<>(String[].class);
		test.add("b");
		test.add("d");
		test.add("a");
		test.parallelSort(String::compareToIgnoreCase);
		Iterator<String> actual = test.iterator();
		for (String s : List.of("a", "b", "d")) {
			assertTrue(actual.hasNext());
			assertEquals(s, actual.next());
		}
		assertFalse(actual.hasNext());
		assertEquals("d", test.getArray()[2]);
	}
}