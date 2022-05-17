package org.rdfhdt.hdt.hdt.impl.diskimport;

import org.rdfhdt.hdt.iterator.utils.ExceptionIterator;
import org.rdfhdt.hdt.triples.IndexedNode;
import org.rdfhdt.hdt.util.io.compress.CompressNodeMergeIterator;
import org.rdfhdt.hdt.util.io.compress.CompressNodeReader;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class CompressionResultPartial implements CompressionResult {
	private final List<CompressNodeReaderTriple> files;
	private final File triples;
	private final long triplesCount;
	private final ExceptionIterator<IndexedNode, IOException> subject;
	private final ExceptionIterator<IndexedNode, IOException> predicate;
	private final ExceptionIterator<IndexedNode, IOException> object;

	public CompressionResultPartial(List<SectionCompressor.TripleFile> files, File triples, long triplesCount) throws IOException {
		this.files = new ArrayList<>(files.size());
		for (SectionCompressor.TripleFile file: files) {
			this.files.add(new CompressNodeReaderTriple(file));
		}
		this.triples = triples;
		this.triplesCount = triplesCount;

		// building iterator trees
		this.subject = createBTree(0, files.size(), CompressNodeReaderTriple::getS);
		this.predicate = createBTree(0, files.size(), CompressNodeReaderTriple::getP);
		this.object = createBTree(0, files.size(), CompressNodeReaderTriple::getO);
	}

	private ExceptionIterator<IndexedNode, IOException> createBTree(int start, int end, Function<CompressNodeReaderTriple, CompressNodeReader> fetcher) {
		int size = end - start;
		if (size <= 0) {
			return ExceptionIterator.empty();
		}
		if (size == 1) {
			return fetcher.apply(files.get(start));
		}
		int mid = (start + end) / 2;
		ExceptionIterator<IndexedNode, IOException> left = createBTree(start, mid, fetcher);
		ExceptionIterator<IndexedNode, IOException> right = createBTree(mid, end, fetcher);
		return new CompressNodeMergeIterator(left, right);
	}

	@Override
	public File getTriples() {
		return triples;
	}

	@Override
	public long getTripleCount() {
		return triplesCount;
	}

	@Override
	public ExceptionIterator<IndexedNode, IOException> getSubjects() {
		return subject;
	}

	@Override
	public ExceptionIterator<IndexedNode, IOException> getPredicates() {
		return predicate;
	}

	@Override
	public ExceptionIterator<IndexedNode, IOException> getObjects() {
		return object;
	}

	@Override
	public void delete() {
		files.forEach(triple -> triple.file.delete());
	}

	@Override
	public void close() throws IOException {
		// close the files
		IOException e = null;
		RuntimeException re = null;
		for (CompressNodeReaderTriple triple : files) {
			try {
				triple.close();
			} catch (IOException ee) {
				e = ee;
			} catch (RuntimeException t) {
				re = t;
			}
		}
		if (e != null) {
			throw e;
		}
		if (re != null) {
			throw re;
		}
	}

	/*
	 * use the count of triples because we don't know the number of subjects
	 */
	@Override
	public long getSubjectsCount() {
		return triplesCount;
	}

	@Override
	public long getPredicatesCount() {
		return triplesCount;
	}

	@Override
	public long getObjectsCount() {
		return triplesCount;
	}

	@Override
	public long getSharedCount() {
		return triplesCount;
	}

	private static class CompressNodeReaderTriple implements Closeable {
		final CompressNodeReader s, p, o;
		final SectionCompressor.TripleFile file;

		public CompressNodeReaderTriple(SectionCompressor.TripleFile file) throws IOException {
			this.s = new CompressNodeReader(file.openRSubject());
			this.p = new CompressNodeReader(file.openRPredicate());
			this.o = new CompressNodeReader(file.openRObject());
			this.file = file;
		}

		@Override
		public void close() throws IOException {
			try {
				s.close();
			} finally {
				try {
					p.close();
				} finally {
					o.close();
				}
			}
		}

		public CompressNodeReader getS() {
			return s;
		}

		public CompressNodeReader getP() {
			return p;
		}

		public CompressNodeReader getO() {
			return o;
		}
	}
}
