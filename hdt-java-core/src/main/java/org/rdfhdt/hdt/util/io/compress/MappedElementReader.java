package org.rdfhdt.hdt.util.io.compress;

import org.rdfhdt.hdt.compact.integer.VByte;
import org.rdfhdt.hdt.iterator.utils.FetcherExceptionIterator;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;

public class MappedElementReader extends FetcherExceptionIterator<MappedId, IOException> implements Closeable {
	private final InputStream stream;
	private final MappedId ids = new MappedId();

	public MappedElementReader(InputStream stream) {
		this.stream = stream;
	}

	@Override
	protected MappedId getNext() throws IOException {
		long tripleID = VByte.decode(stream, true);

		if (tripleID == -1) {
			// eof
			return null;
		}

		long mappedID = VByte.decode(stream);
		ids.setAll(tripleID, mappedID);
		return ids;
	}

	@Override
	public void close() throws IOException {
		stream.close();
	}
}
