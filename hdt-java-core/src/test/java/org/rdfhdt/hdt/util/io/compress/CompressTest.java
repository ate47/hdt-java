package org.rdfhdt.hdt.util.io.compress;

import org.junit.Assert;
import org.junit.Test;
import org.rdfhdt.hdt.iterator.utils.ExceptionIterator;
import org.rdfhdt.hdt.triples.IndexedNode;
import org.rdfhdt.hdt.util.string.CharSequenceComparator;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class CompressTest {
	public static void assertCharSequenceEquals(String location, CharSequence s1, CharSequence s2) {
		if (CharSequenceComparator.getInstance().compare(s1, s2) != 0) {
			throw new AssertionError(location + ", excepted: " + s1 + " != actual: " + s2);
		}
	}

	@Test
	public void noDupeTest() {
		List<IndexedNode> duplicatedList = Arrays.asList(
				new IndexedNode("a", 0),
				new IndexedNode("b", 1),
				new IndexedNode("b", 2),
				new IndexedNode("c", 3),
				new IndexedNode("c", 4),
				new IndexedNode("c", 5),
				new IndexedNode("d", 6),
				new IndexedNode("e", 7),
				new IndexedNode("f", 8)
		);
		List<CharSequence> noDuplicatedList = Arrays.asList(
				"a",
				"b",
				"c",
				"d",
				"e",
				"f"
		);

		Set<Long> duplicates = new HashSet<>();
		duplicates.add(2L);
		duplicates.add(4L);
		duplicates.add(5L);

		Iterator<IndexedNode> actual = CompressUtil.asNoDupeCharSequenceIterator(
				ExceptionIterator.of(duplicatedList.iterator()),
				(originalIndex, duplicatedIndex) ->
						Assert.assertTrue(duplicates.remove(duplicatedIndex))
		);
		for (CharSequence e : noDuplicatedList) {
			Assert.assertTrue(actual.hasNext());
			CharSequence a = actual.next().getNode();

			assertCharSequenceEquals("noDupeTest", e, a);
		}
	}

	@Test
	public void bitMappingTest() {
		long sharedCount = 1000L;
		long index1 = 888L;

		long sharedIndex1 = CompressUtil.asShared(index1);

		Assert.assertEquals(index1, CompressUtil.computeSharedNode(sharedIndex1, sharedCount));
		Assert.assertEquals(sharedCount + index1, CompressUtil.computeSharedNode(index1, sharedCount));

		long dupeIndex1 = CompressUtil.asDuplicated(index1);
		long dupeSharedIndex1 = CompressUtil.asDuplicated(sharedIndex1);

		Assert.assertTrue(CompressUtil.isDuplicated(dupeIndex1));
		Assert.assertTrue(CompressUtil.isDuplicated(dupeSharedIndex1));

		Assert.assertEquals(index1, CompressUtil.getDuplicatedIndex(dupeIndex1));
		Assert.assertEquals(sharedIndex1, CompressUtil.getDuplicatedIndex(dupeSharedIndex1));
	}
}
