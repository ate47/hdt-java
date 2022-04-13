package org.rdfhdt.hdt.util.io;

import org.rdfhdt.hdt.compact.integer.VByte;
import org.rdfhdt.hdt.dictionary.impl.section.PFCDictionarySection;
import org.rdfhdt.hdt.options.HDTOptions;
import org.rdfhdt.hdt.triples.TripleString;
import org.rdfhdt.hdt.util.crc.CRC32;
import org.rdfhdt.hdt.util.crc.CRC8;
import org.rdfhdt.hdt.util.crc.CRCOutputStream;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.List;

public class CompressNodeWriter {
	public static void writeCompress(List<CharSequence> strings, OutputStream output, HDTOptions spec) throws IOException {
		int blocksize = (int) spec.getInt("pfc.blocksize");
		if(blocksize==0) {
			blocksize = PFCDictionarySection.DEFAULT_BLOCK_SIZE;
		}
		CRCOutputStream out = new CRCOutputStream(output, new CRC8());
		VByte.encode(output, strings.size());
		Iterator<CharSequence> it = strings.iterator();
		CharSequence previousStr = it.next();
		for (CharSequence sec : strings) {

		}
	}

	public static void writeCompress(TripleString triple, FileWriter writer) throws IOException {
		triple.dumpNtriple(writer);
	}

	public static void mergeCompress(InputStream stream1, InputStream stream2, HDTOptions spec, OutputStream output) throws IOException {
		CRCOutputStream out = new CRCOutputStream(output, new CRC8());
		long size1 = VByte.decode(stream1);
		long size2 = VByte.decode(stream2);
		// write the size of the future block
		VByte.encode(out, size1 + size2);

		out.writeCRC();
		out.setCRC(new CRC32());
	}
}
