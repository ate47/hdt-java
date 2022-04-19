package org.rdfhdt.hdt.hdt.impl.diskimport;

import org.rdfhdt.hdt.enums.TripleComponentOrder;
import org.rdfhdt.hdt.iterator.utils.ExceptionIterator;
import org.rdfhdt.hdt.triples.TempTriples;
import org.rdfhdt.hdt.triples.TripleID;
import org.rdfhdt.hdt.triples.impl.OneReadTempTriples;
import org.rdfhdt.hdt.util.io.compress.CompressTripleMergeIterator;
import org.rdfhdt.hdt.util.io.compress.CompressTripleReader;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TripleCompressionResultPartial implements TripleCompressionResult {
	private final List<CompressTripleReader> files;
	private final TempTriples triples;
	private final long tripleCount;
	private final TripleComponentOrder order;

	public TripleCompressionResultPartial(List<File> files, long tripleCount, TripleComponentOrder order) throws IOException {
		this.files = new ArrayList<>(files.size());
		this.tripleCount = tripleCount;
		this.order = order;
		this.triples = new OneReadTempTriples(createBTree(files, 0, files.size()).asIterator(), order, tripleCount);
	}
	private ExceptionIterator<TripleID, IOException> createBTree(List<File> files, int start, int end) throws IOException {
		int size = end - start;
		if (size <= 0) {
			return ExceptionIterator.empty();
		}
		if (size == 1) {
			CompressTripleReader r = new CompressTripleReader(new FileInputStream(files.get(start)));
			this.files.add(r);
			return r;
		}
		int mid = (start + end) / 2;
		ExceptionIterator<TripleID, IOException> left = createBTree(files, start, mid);
		ExceptionIterator<TripleID, IOException> right = createBTree(files, mid, end);
		return new CompressTripleMergeIterator(left, right, order);
	}

	@Override
	public TempTriples getTriples() {
		return triples;
	}

	@Override
	public long getTripleCount() {
		return tripleCount;
	}

	@Override
	public void close() throws IOException {
		// close the files
		IOException e = null;
		RuntimeException re = null;
		for (Closeable triple : files) {
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
}
