package org.rdfhdt.hdt.util.io.compress;

import org.rdfhdt.hdt.compact.integer.VByte;
import org.rdfhdt.hdt.triples.IndexedTriple;
import org.rdfhdt.hdt.util.crc.CRC32;
import org.rdfhdt.hdt.util.crc.CRCOutputStream;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Class to write pre-mapped triples file
 *
 * @author Antoine Willerval
 */
public class CompressTripleWriter implements Closeable {
	private final CRCOutputStream out;

	public CompressTripleWriter(OutputStream writer) {
		this.out = new CRCOutputStream(writer, new CRC32());
	}
	/**
	 * write a indexed triple into an output
	 * @param triple the triple to write
	 * @throws java.io.IOException write exception
	 */
	public void appendTriple(IndexedTriple triple) throws IOException {
		VByte.encode(out, triple.getSubject().getIndex());
		VByte.encode(out, triple.getPredicate().getIndex());
		VByte.encode(out, triple.getObject().getIndex());
	}

	@Override
	public void close() throws IOException {
		VByte.encode(out, 0);
		VByte.encode(out, 0);
		VByte.encode(out, 0);
		out.writeCRC();
	}
}
