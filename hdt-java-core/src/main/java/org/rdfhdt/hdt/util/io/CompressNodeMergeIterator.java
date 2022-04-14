package org.rdfhdt.hdt.util.io;

import org.rdfhdt.hdt.iterator.utils.ExceptionIterator;
import org.rdfhdt.hdt.triples.IndexedNode;

import java.io.IOException;

public class CompressNodeMergeIterator implements ExceptionIterator<IndexedNode, IOException> {
	private final CompressNodeReader in1, in2;
	private IndexedNode next;

	public CompressNodeMergeIterator(CompressNodeReader in1, CompressNodeReader in2) {
		this.in1 = in1;
		this.in2 = in2;
	}

	@Override
	public boolean hasNext() throws IOException {
		if (next != null) {
			return true;
		}

		if (in1.hasNext() && in2.hasNext()) {
			IndexedNode e1 = in1.read();
			IndexedNode e2 = in2.read();
			if (e1.compareTo(e2) < 0) {
				next = e1;
				in1.pass();
			} else {
				next = e2;
				in2.pass();
			}
			return true;
		}
		if (in1.hasNext()) {
			next = in1.read();
			in1.pass();
			return true;
		}
		if (in2.hasNext()) {
			next = in2.read();
			in2.pass();
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
