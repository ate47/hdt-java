package org.rdfhdt.hdt.dictionary.impl;

import org.junit.Assert;
import org.junit.Test;
import org.rdfhdt.hdt.exceptions.NotImplementedException;
import org.rdfhdt.hdt.hdt.impl.diskimport.CompressionResult;
import org.rdfhdt.hdt.iterator.utils.ExceptionIterator;
import org.rdfhdt.hdt.iterator.utils.MapIterator;
import org.rdfhdt.hdt.triples.IndexedNode;
import org.rdfhdt.hdt.util.concurrent.ExceptionThread;
import org.rdfhdt.hdt.util.io.compress.CompressUtil;
import org.rdfhdt.hdt.util.string.CharSequenceComparator;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class CompressFourSectionDictionaryTest {
	private void assertCharSequenceEquals(String location, CharSequence s1, CharSequence s2) {
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
	public void compressDictTest() throws Exception {
		TestCompressionResult result = new TestCompressionResult(
				new CharSequence[]{
						"2222", "4444", "5555", "7777", "9999", "9999"
				},
				new CharSequence[]{
						"1111", "1111", "2222", "3333", "3333", "4444"
				},
				new CharSequence[]{
						"1111", "3333", "3333", "4444", "6666", "7777", "8888"
				}
		);
		List<CharSequence> exceptedSubjects = Arrays.asList(
				"2222", "5555", "9999"
		);
		List<CharSequence> exceptedPredicates = Arrays.asList(
				"1111", "2222", "3333", "4444"
		);
		List<CharSequence> exceptedObjects = Arrays.asList(
				"1111", "3333", "6666", "8888"
		);
		List<CharSequence> exceptedShared = Arrays.asList(
				"4444", "7777"
		);
		CompressFourSectionDictionary dictionary = new CompressFourSectionDictionary(result, new FakeNodeConsumer());
		Iterator<? extends CharSequence> su = dictionary.getSubjects().getSortedEntries();
		Iterator<? extends CharSequence> pr = dictionary.getPredicates().getSortedEntries();
		Iterator<? extends CharSequence> ob = dictionary.getObjects().getSortedEntries();
		Iterator<? extends CharSequence> sh = dictionary.getShared().getSortedEntries();
		ExceptionThread subjectReader = new ExceptionThread(() -> {
			for (CharSequence e : exceptedSubjects) {
				Assert.assertTrue(su.hasNext());
				CharSequence a = su.next();
				Thread.sleep(40);
				assertCharSequenceEquals("Subject", e, a);
			}
		}, "compressDictTestS");
		ExceptionThread predicateReader = new ExceptionThread(() -> {
			for (CharSequence e : exceptedPredicates) {
				Assert.assertTrue(pr.hasNext());
				CharSequence a = pr.next();
				Thread.sleep(40);
				assertCharSequenceEquals("Predicate", e, a);
			}
		}, "compressDictTestP");
		ExceptionThread objectReader = new ExceptionThread(() -> {
			for (CharSequence e : exceptedObjects) {
				Assert.assertTrue(ob.hasNext());
				CharSequence a = ob.next();
				Thread.sleep(40);
				assertCharSequenceEquals("Object", e, a);
			}
		}, "compressDictTestO");
		ExceptionThread sharedReader = new ExceptionThread(() -> {
			for (CharSequence e : exceptedShared) {
				Assert.assertTrue(sh.hasNext());
				CharSequence a = sh.next();
				Thread.sleep(40);
				assertCharSequenceEquals("Shared", e, a);
			}
		}, "compressDictTestSh");

		sharedReader.attach(
				predicateReader,
				objectReader,
				subjectReader
		).startAll().joinAndCrashIfRequired();
	}

	static class TestCompressionResult implements CompressionResult {
		private final CharSequence[] subjects;
		private final CharSequence[] predicates;
		private final CharSequence[] objects;

		public TestCompressionResult(CharSequence[] subjects, CharSequence[] predicates, CharSequence[] objects) {
			this.subjects = subjects;
			this.predicates = predicates;
			this.objects = objects;
		}

		@Override
		public File getTriples() {
			throw new NotImplementedException();
		}

		@Override
		public long getTripleCount() {
			throw new NotImplementedException();
		}

		@Override
		public ExceptionIterator<IndexedNode, IOException> getSubjects() {
			return ExceptionIterator.of(new MapIterator<>(Arrays.asList(subjects).iterator(), s -> new IndexedNode(s, 0)));
		}

		@Override
		public ExceptionIterator<IndexedNode, IOException> getPredicates() {
			return ExceptionIterator.of(new MapIterator<>(Arrays.asList(predicates).iterator(), s -> new IndexedNode(s, 0)));
		}

		@Override
		public ExceptionIterator<IndexedNode, IOException> getObjects() {
			return ExceptionIterator.of(new MapIterator<>(Arrays.asList(objects).iterator(), s -> new IndexedNode(s, 0)));
		}

		@Override
		public long getSubjectsCount() {
			return subjects.length;
		}

		@Override
		public long getPredicatesCount() {
			return predicates.length;
		}

		@Override
		public long getObjectsCount() {
			return objects.length;
		}

		@Override
		public long getSharedCount() {
			return Math.min(subjects.length, objects.length);
		}

		@Override
		public void delete() {
		}

		@Override
		public void close() {
		}
	}

	static class FakeNodeConsumer implements CompressFourSectionDictionary.NodeConsumer {
		@Override
		public void onSubject(long preMapId, long newMapId) {
		}

		@Override
		public void onPredicate(long preMapId, long newMapId) {
		}

		@Override
		public void onObject(long preMapId, long newMapId) {
		}
	}
}
