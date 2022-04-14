package org.rdfhdt.hdt.util.io;

import org.rdfhdt.hdt.compact.integer.VByte;
import org.rdfhdt.hdt.exceptions.CRCException;
import org.rdfhdt.hdt.iterator.utils.ExceptionIterator;
import org.rdfhdt.hdt.triples.TripleID;
import org.rdfhdt.hdt.util.crc.CRC32;
import org.rdfhdt.hdt.util.crc.CRCInputStream;
import org.rdfhdt.hdt.util.disk.LongArrayDisk;

import java.io.IOException;
import java.io.InputStream;

/**
 * Class to read and map pre-mapped a triples file
 *
 * @author Antoine Willerval
 */
public class CompressTripleReader implements ExceptionIterator<TripleID, IOException> {
	private final CRCInputStream stream;
	private final TripleID next = new TripleID(-1, -1, -1);
	private boolean read = false, end = false;
	private final LongArrayDisk sMapper;
	private final LongArrayDisk pMapper;
	private final LongArrayDisk oMapper;
	private final long shared;

	public CompressTripleReader(InputStream stream, LongArrayDisk sMapper, LongArrayDisk pMapper, LongArrayDisk oMapper, long shared) {
		this.stream = new CRCInputStream(stream, new CRC32());
		this.sMapper = sMapper;
		this.pMapper = pMapper;
		this.oMapper = oMapper;
		this.shared = shared;
	}

	@Override
	public boolean hasNext() throws IOException {
		if (read) {
			return true;
		}

		// the reader is empty, null end triple
		if (end) {
			return false;
		}

		long s = VByte.decode(stream);
		long p = VByte.decode(stream);
		long o = VByte.decode(stream);

		return !setAllOrEnd(s, p, o);
	}

	private boolean setAllOrEnd(long s, long p, long o) throws IOException {
		if (end) {
			// already completed
			return true;
		}
		if (s == 0 || p == 0 || o == 0) {
			// check triples validity
			if (s != 0 || p != 0 || o != 0) {
				throw new IOException("Triple got null node, but not all the nodes are 0!" + next);
			}
			if (!stream.readCRCAndCheck()) {
				throw new CRCException("CRC Error while reading PreMapped triples.");
			}
			// set to true to avoid reading again the CRC
			end = true;
			return true;
		}
		// map the triples to the end id, compute the shared with the end shared size
		next.setAll(
				CompressNodeWriter.computeSharedNode(sMapper.get(s), shared),
				pMapper.get(p),
				CompressNodeWriter.computeSharedNode(oMapper.get(o), shared)
		);
		read = true;
		return false;
	}

	@Override
	public TripleID next() throws IOException {
		if (!hasNext()) {
			return null;
		}
		return next;
	}
}
