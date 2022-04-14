package org.rdfhdt.hdt.util.io;

import org.rdfhdt.hdt.compact.integer.VByte;
import org.rdfhdt.hdt.iterator.utils.ExceptionIterator;
import org.rdfhdt.hdt.triples.IndexedNode;
import org.rdfhdt.hdt.util.string.ReplazableString;

import java.io.IOException;
import java.io.InputStream;

public class CompressNodeReader implements ExceptionIterator<IndexedNode, IOException> {
	private final InputStream stream;
	private final long size;
	private final long blockSize;
	private long index;
	private IndexedNode last;
	private final ReplazableString tempString = new ReplazableString();

	public CompressNodeReader(InputStream stream, long size, long blockSize) {
		this.stream = stream;
		this.size = size;
		this.blockSize = blockSize;
	}

	/**
	 * @return the next element without passing to the next element
	 * @throws IOException reading exception
	 */
	public IndexedNode read() throws IOException {
		if (last != null) {
			return last;
		}
		int delta;
		// read the string
		if (index % blockSize != 0) {
			// not a new block, has delta
			delta = (int) VByte.decode(stream);
		} else {
			delta = 0;
		}
		tempString.replace2(stream, delta);
		long index = VByte.decode(stream);
		last = new IndexedNode(tempString, index);
		return last;
	}

	/**
	 * pass to the next element, mandatory with {@link #read()}
	 */
	public void pass() {
		last = null;
		index++;
	}

	@Override
	public IndexedNode next() throws IOException {
		IndexedNode node = read();
		pass();
		return node;
	}
	@Override
	public boolean hasNext() throws IOException {
		return index < size;
	}
}
