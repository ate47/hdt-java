package org.rdfhdt.hdt.util.io;

import org.rdfhdt.hdt.compact.integer.VByte;
import org.rdfhdt.hdt.dictionary.impl.section.PFCDictionarySection;
import org.rdfhdt.hdt.exceptions.CRCException;
import org.rdfhdt.hdt.iterator.utils.ExceptionIterator;
import org.rdfhdt.hdt.options.HDTOptions;
import org.rdfhdt.hdt.triples.IndexedNode;
import org.rdfhdt.hdt.triples.IndexedTriple;
import org.rdfhdt.hdt.util.crc.CRC32;
import org.rdfhdt.hdt.util.crc.CRC8;
import org.rdfhdt.hdt.util.crc.CRCInputStream;
import org.rdfhdt.hdt.util.crc.CRCOutputStream;
import org.rdfhdt.hdt.util.string.ByteStringUtil;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

public class CompressNodeWriter {
	/**
	 * write a sorted list of indexed node
	 * @param strings the nodes to write
	 * @param output the output
	 * @param spec the spec for the writing
	 * @throws IOException writing exception
	 */
	public static void writeCompress(List<IndexedNode> strings, OutputStream output, HDTOptions spec) throws IOException {
		int blocksize = (int) spec.getInt("pfc.blocksize");
		if (blocksize == 0) {
			blocksize = PFCDictionarySection.DEFAULT_BLOCK_SIZE;
		}
		write(ExceptionIterator.of(strings.iterator()), strings.size(), blocksize, output);
	}

	/**
	 * write a sorted iterator of indexed node
	 * @param it iterator to write
	 * @param size size of the iterator
	 * @param blocksize the size of a block
	 * @param output the output where to write
	 * @throws IOException writing exception
	 */
	public static void write(ExceptionIterator<IndexedNode, IOException> it, long size, long blocksize, OutputStream output) throws IOException {
		CRCOutputStream out = new CRCOutputStream(output, new CRC8());
		VByte.encode(out, size);
		VByte.encode(out, blocksize);

		out.writeCRC();
		out.setCRC(new CRC32());
		long numstrings = 0;
		CharSequence previousStr = null;

		while (it.hasNext()) {
			IndexedNode next = it.next();
			CharSequence str = next.getNode();
			long index = next.getIndex();

			if (numstrings % blocksize == 0) {
				// Copy full string
				ByteStringUtil.append(out, str, 0);
			} else {
				// Find common part.
				int delta = ByteStringUtil.longestCommonPrefix(previousStr, str);
				// Write Delta in VByte
				VByte.encode(out, delta);
				// Write remaining
				ByteStringUtil.append(out, str, delta);
			}
			out.write(0); // End of string
			VByte.encode(out, index); // index of the node

			numstrings++;
			previousStr = str;
		}
		// complete the section
		out.writeCRC();
	}

	/**
	 * write a indexed triple into an output
	 * @param triple the triple to write
	 * @param out the output stream to write
	 * @throws IOException write exception
	 */
	public static void writeCompress(IndexedTriple triple, OutputStream out) throws IOException {
		VByte.encode(out, triple.getSubject().getIndex());
		VByte.encode(out, triple.getPredicate().getIndex());
		VByte.encode(out, triple.getObject().getIndex());
	}
	/**
	 * write the end of an output triple, aka triple (0, 0, 0)
	 * @param out the output stream to write
	 * @throws IOException write exception
	 */
	public static void writeEndTriple(OutputStream out) throws IOException {
		VByte.encode(out, 0);
		VByte.encode(out, 0);
		VByte.encode(out, 0);
	}

	/**
	 * merge two stream together into an output stream
	 * @param stream1 input stream 1
	 * @param stream2 input stream 2
	 * @param output output stream
	 * @throws IOException read/writing exception
	 */
	public static void mergeCompress(InputStream stream1, InputStream stream2, OutputStream output) throws IOException {
		CRCInputStream in1 = new CRCInputStream(stream1, new CRC8());
		CRCInputStream in2 = new CRCInputStream(stream2, new CRC8());

		long size1 = VByte.decode(in1);
		long blockSize1 = VByte.decode(in1);
		long size2 = VByte.decode(in2);
		long blockSize2 = VByte.decode(in2);

		if(!in1.readCRCAndCheck() || !in2.readCRCAndCheck()) {
			throw new CRCException("CRC Error while merging Section Plain Front Coding Header.");
		}

		in1.setCRC(new CRC32());
		in2.setCRC(new CRC32());

		long blockSize = Math.max(blockSize1, blockSize2); // bs1 != bs2 shouldn't be possible?
		
		CRCOutputStream out = new CRCOutputStream(output, new CRC8());
		// write the size of the future block
		VByte.encode(out, size1 + size2);
		VByte.encode(out, blockSize);
		out.writeCRC();
		out.setCRC(new CRC32());
		CompressNodeReader in1r = new CompressNodeReader(in1, size1, blockSize1);
		CompressNodeReader in2r = new CompressNodeReader(in2, size2, blockSize2);
		// merge the section
		write(new CompressNodeMergeIterator(in1r, in2r), size1 + size2, blockSize, output);
		if(!in1.readCRCAndCheck() || !in2.readCRCAndCheck()) {
			throw new CRCException("CRC Error while merging Section Plain Front Coding Header.");
		}
	}

	public static void computeShared(String sectionFile1, String sectionFile2, String sharedSectionFile) throws IOException {

	}
}
