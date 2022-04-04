package org.rdfhdt.hdt.util.io;

import org.rdfhdt.hdt.dictionary.impl.section.PFCDictionarySection;
import org.rdfhdt.hdt.options.HDTOptions;
import org.rdfhdt.hdt.triples.TripleString;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class CompressNodeWriter {
	public static void writeCompress(List<CharSequence> strings, File f, HDTOptions spec) throws IOException {
		try (FileWriter writer = new FileWriter(f)) {
			int blocksize = (int) spec.getInt("pfc.blocksize");
			if(blocksize==0) {
				blocksize = PFCDictionarySection.DEFAULT_BLOCK_SIZE;
			}

			Iterator<CharSequence> it = strings.iterator();
			CharSequence previousStr=it.next();
			for (CharSequence sec : strings) {
			}
		}
	}

	public static void writeCompress(TripleString triple, FileWriter writer) throws IOException {
		triple.dumpNtriple(writer);
	}
}
