package org.rdfhdt.hdt.hdt.impl.diskimport;

import org.rdfhdt.hdt.enums.TripleComponentOrder;
import org.rdfhdt.hdt.triples.TempTriples;
import org.rdfhdt.hdt.triples.impl.OneReadTempTriples;
import org.rdfhdt.hdt.util.io.compress.CompressTripleReader;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;

public class TripleCompressionResultFile implements TripleCompressionResult {
	private final long tripleCount;
	private final CompressTripleReader reader;
	private final TripleComponentOrder order;
	private final File triples;

	public TripleCompressionResultFile(long tripleCount, File triples, TripleComponentOrder order) throws IOException {
		this.tripleCount = tripleCount;
		this.reader = new CompressTripleReader(new FileInputStream(triples));
		this.order = order;
		this.triples = triples;
	}

	@Override
	public TempTriples getTriples() {
		return new OneReadTempTriples(reader.asIterator(), order, tripleCount);
	}

	@Override
	public long getTripleCount() {
		return tripleCount;
	}

	@Override
	public void close() throws IOException {
		try {
			reader.close();
		} finally {
			Files.deleteIfExists(triples.toPath());
		}
	}
}
