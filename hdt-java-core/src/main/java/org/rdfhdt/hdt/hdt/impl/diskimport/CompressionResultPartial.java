package org.rdfhdt.hdt.hdt.impl.diskimport;

import org.rdfhdt.hdt.exceptions.NotImplementedException;
import org.rdfhdt.hdt.iterator.utils.ExceptionIterator;
import org.rdfhdt.hdt.triples.IndexedNode;
import org.rdfhdt.hdt.util.io.IOUtil;
import org.rdfhdt.hdt.util.io.compress.CompressNodeMergeIterator;
import org.rdfhdt.hdt.util.io.compress.CompressNodeReader;
import org.rdfhdt.hdt.util.io.compress.MappedId;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * Implementation of {@link org.rdfhdt.hdt.hdt.impl.diskimport.CompressionResult} for partial file reading
 *
 * @author Antoine Willerval
 */
public class CompressionResultPartial implements CompressionResult {
	private final List<CompressNodeReaderTriple> files;
	private final long triplesCount;
	private final long ntSize;
	private final ExceptionIterator<IndexedNode, IOException> subject;
	private final ExceptionIterator<IndexedNode, IOException> predicate;
	private final ExceptionIterator<IndexedNode, IOException> object;

	public CompressionResultPartial(List<SectionCompressor.TripleFile> files, long triplesCount, long ntSize) throws IOException {
		this.files = new ArrayList<>(files.size());
		this.ntSize = ntSize;
		for (SectionCompressor.TripleFile file : files) {
			this.files.add(new CompressNodeReaderTriple(file));
		}
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
	public ExceptionIterator<MappedId, IOException> getMappedIdSubjects() {
		throw new NotImplementedException("mapped id");
	}

	@Override
	public ExceptionIterator<MappedId, IOException> getMappedIdPredicates() {
		throw new NotImplementedException("mapped id");
	}

	@Override
	public ExceptionIterator<MappedId, IOException> getMappedIdObjects() {
		throw new NotImplementedException("mapped id");
	}

	@Override
	public void delete() throws IOException {
		IOUtil.closeAll(files);
	}

	@Override
	public void close() throws IOException {
		IOUtil.closeAll(files);
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

	@Override
	public long getRawSize() {
		return ntSize;
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
			IOUtil.closeAll(s, p, o);
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
