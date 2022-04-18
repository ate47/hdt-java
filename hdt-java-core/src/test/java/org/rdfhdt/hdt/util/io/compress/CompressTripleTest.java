package org.rdfhdt.hdt.util.io.compress;

import org.junit.Assert;
import org.junit.Test;
import org.rdfhdt.hdt.enums.TripleComponentOrder;
import org.rdfhdt.hdt.exceptions.NotImplementedException;
import org.rdfhdt.hdt.iterator.utils.ExceptionIterator;
import org.rdfhdt.hdt.triples.IndexedNode;
import org.rdfhdt.hdt.triples.IndexedTriple;
import org.rdfhdt.hdt.triples.TripleID;
import org.rdfhdt.hdt.util.concurrent.ExceptionThread;
import org.rdfhdt.hdt.util.disk.LongArray;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class CompressTripleTest {
	@Test
	public void writeReadTest() throws InterruptedException, IOException {
		PipedOutputStream out = new PipedOutputStream();
		PipedInputStream in = new PipedInputStream();
		out.connect(in);
		List<IndexedTriple> triples = Arrays.asList(
				new IndexedTriple(
						new IndexedNode("bob", 1),
						new IndexedNode("predi1", 9),
						new IndexedNode("obj1", 11)
				),
				new IndexedTriple(
						new IndexedNode("michel", 3),
						new IndexedNode("predi", 10),
						new IndexedNode("obj2", 11)
				),
				new IndexedTriple(
						new IndexedNode("jack", 2),
						new IndexedNode("predi2", 12),
						new IndexedNode("obj3", 15)
				),
				new IndexedTriple(
						new IndexedNode("charles", 6),
						new IndexedNode("predi3", 14),
						new IndexedNode("obj4", 13)
				)
		);
		new ExceptionThread(() -> {
			CompressTripleReader reader = new CompressTripleReader(
					in,
					new NonMutableLongArray(triples.size()),
					new NonMutableLongArray(triples.size()),
					new NonMutableLongArray(triples.size()),
					0
			);
			try {
				for (IndexedTriple exceptedIndex : triples) {
					Assert.assertTrue(reader.hasNext());
					TripleID actual = reader.next();
					TripleID excepted = new TripleID(
							exceptedIndex.getSubject().getIndex(),
							exceptedIndex.getPredicate().getIndex(),
							exceptedIndex.getObject().getIndex()
					);
					Assert.assertEquals(excepted, actual);
				}
				Assert.assertFalse(reader.hasNext());
				Assert.assertEquals(34, in.read());
				Assert.assertEquals(12, in.read());
				Assert.assertEquals(27, in.read());
			} finally {
				in.close();
			}
		}, "ReadTest").attach(
				new ExceptionThread(() -> {
					CompressTripleWriter writer = new CompressTripleWriter(out);
					try {
						for (IndexedTriple triple : triples) {
							writer.appendTriple(triple);
						}
						writer.writeCRC();
						// raw data to check if we didn't read too/not enough data
						out.write(34);
						out.write(12);
						out.write(27);
					} finally {
						out.close();
					}
				}, "WriteTest")
		).startAll().joinAndCrashIfRequired();
	}
	@Test
	public void writeReadMergeTest() {
		List<TripleID> triples1 = Arrays.asList(
				new TripleID(2, 2, 2),
				new TripleID(4, 4, 4),
				new TripleID(5, 5, 5)
		);
		List<TripleID> triples2 = Arrays.asList(
				new TripleID(1, 1, 1),
				new TripleID(3, 3, 3),
				new TripleID(6, 6, 6)
		);
		List<TripleID> triplesFinal = Arrays.asList(
				new TripleID(1, 1, 1),
				new TripleID(2, 2, 2),
				new TripleID(3, 3, 3),
				new TripleID(4, 4, 4),
				new TripleID(5, 5, 5),
				new TripleID(6, 6, 6)
		);
		Iterator<TripleID> actual = new CompressTripleMergeIterator(
				ExceptionIterator.of(triples1.iterator()),
				ExceptionIterator.of(triples2.iterator()),
				TripleComponentOrder.SPO
		).asIterator();
		Iterator<TripleID> expected = triplesFinal.iterator();

		expected.forEachRemaining(tid -> {
			Assert.assertTrue(actual.hasNext());
			Assert.assertEquals(tid, actual.next());
		});
		Assert.assertFalse(actual.hasNext());

	}

	private static class NonMutableLongArray implements LongArray {
		private final long size;

		public NonMutableLongArray(long size) {
			this.size = size;
		}

		@Override
		public long get(long x) {
			return x;
		}

		@Override
		public void set(long x, long y) {
			throw new NotImplementedException();
		}

		@Override
		public long length() {
			return size;
		}
	}

}
