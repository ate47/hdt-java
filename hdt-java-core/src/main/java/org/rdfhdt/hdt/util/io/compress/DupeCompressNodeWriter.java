package org.rdfhdt.hdt.util.io.compress;

import org.rdfhdt.hdt.compact.integer.VByte;
import org.rdfhdt.hdt.triples.IndexedNode;
import org.rdfhdt.hdt.util.crc.CRC32;
import org.rdfhdt.hdt.util.crc.CRC8;
import org.rdfhdt.hdt.util.crc.CRCOutputStream;
import org.rdfhdt.hdt.util.string.ByteString;
import org.rdfhdt.hdt.util.string.ByteStringUtil;
import org.rdfhdt.hdt.util.string.ReplazableString;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

/**
 * {@link CompressNodeWriter} implementation allowing to get the IDs of the duplicated elements,
 * this writer is using a dynamic size, YOU NEED TO CLOSE IT if you want to make the file readable
 * by a {@link CompressNodeReader}.
 *
 * @author Antoine Willerval
 */
public class DupeCompressNodeWriter implements Closeable {
	private final FileOutputStream fileOutputStream;
	private final CRCOutputStream out;
	private final ReplazableString previousStr = new ReplazableString();
	private long lastIndex;
	private long size;
	private final DupeCallback dupeCallback;

	public DupeCompressNodeWriter(Path file, DupeCallback dupeCallback) throws IOException {
		fileOutputStream = new FileOutputStream(file.toFile());
		out = new CRCOutputStream(fileOutputStream, new CRC8());
		this.dupeCallback = dupeCallback;
		// we don't know the size yet, using padded VByte because of laziness
		VByte.encodePadded(this.out, 0);
		this.out.writeCRC();
		this.out.setCRC(new CRC32());
	}

	public void appendNode(IndexedNode node) throws IOException {
		ByteString str = node.getNode();
		long index = node.getIndex();

		// Find common part.
		int delta = ByteStringUtil.longestCommonPrefix(previousStr, str);

		if (delta == str.length() && delta == previousStr.length()) {
			// same string as the previous one
			dupeCallback.onDupe(index, lastIndex);
		} else {
			// Write Delta in VByte
			VByte.encode(out, delta);
			// Write remaining
			ByteStringUtil.append(out, str, delta);
			out.write(0); // End of string
			VByte.encode(out, index); // index of the node
			lastIndex = index;
			previousStr.replace(str);
			size++;
		}
	}

	public void writeCRCAndSize() throws IOException {
		out.writeCRC();
		// 16 is enough for the CRC+padded VByte
		ByteArrayOutputStream stream = new ByteArrayOutputStream(16);
		CRCOutputStream crcOutputStream = new CRCOutputStream(stream, new CRC8());
		VByte.encodePadded(crcOutputStream, size);
		crcOutputStream.writeCRC();
		FileChannel channel = fileOutputStream.getChannel();
		// set the location to 0
		channel.position(0);
		channel.write(ByteBuffer.wrap(stream.toByteArray()));
	}

	@Override
	public void close() throws IOException {
		try {
			writeCRCAndSize();
		} finally {
			out.close();
		}
	}

	/**
	 * dupe callback
	 */
	public interface DupeCallback {
		/**
		 * dupe
		 * @param tripleId current triple ID
		 * @param mappedId mapped ID for this triple
		 */
		void onDupe(long tripleId, long mappedId) throws IOException;
	}
}