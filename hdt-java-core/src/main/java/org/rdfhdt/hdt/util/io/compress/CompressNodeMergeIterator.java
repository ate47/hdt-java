package org.rdfhdt.hdt.util.io.compress;

import org.rdfhdt.hdt.iterator.utils.ExceptionIterator;
import org.rdfhdt.hdt.triples.IndexedNode;

import java.io.IOException;

/**
 * Iterator to merge two ExceptionIterator of IndexedNode
 * @author Antoine Willerval
 */
public class CompressNodeMergeIterator implements ExceptionIterator<IndexedNode, IOException> {
	private final ExceptionIterator<IndexedNode, IOException> in1, in2;
	private IndexedNode next;
	private IndexedNode prevE1;
	private IndexedNode prevE2;

	public CompressNodeMergeIterator(ExceptionIterator<IndexedNode, IOException> in1, ExceptionIterator<IndexedNode, IOException> in2) {
		this.in1 = in1;
		this.in2 = in2;
	}

	@Override
	public boolean hasNext() throws IOException {
		if (next != null) {
			return true;
		}

		if (prevE1 == null && in1.hasNext()) {
			prevE1 = in1.next();
		}
		if (prevE2 == null && in2.hasNext()) {
			prevE2 = in2.next();
		}

		if (prevE1 != null && prevE2 != null) {
			if (prevE1.compareTo(prevE2) < 0) {
				next = prevE1;
				prevE1 = null;
			} else {
				next = prevE2;
				prevE2 = null;
			}
			return true;
		}
		if (prevE1 != null) {
			next = prevE1;
			prevE1 = null;
			return true;
		}
		if (prevE2 != null) {
			next = prevE2;
			prevE2 = null;
			return true;
		}
		return false;
	}

	@Override
	public IndexedNode next() throws IOException {
		if (!hasNext()) {
			return null;
		}
		IndexedNode next = this.next;
		this.next = null;
		return next;
	}
}
