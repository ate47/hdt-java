package org.rdfhdt.hdt.hdt.impl.diskimport;

import org.rdfhdt.hdt.iterator.utils.ExceptionIterator;
import org.rdfhdt.hdt.triples.IndexedNode;
import org.rdfhdt.hdt.util.io.compress.CompressNodeReader;

import java.io.File;
import java.io.IOException;

public class CompressionResultFile implements CompressionResult {
	private final File triples;
	private final long tripleCount;
	private final CompressNodeReader subjects;
	private final CompressNodeReader predicates;
	private final CompressNodeReader objects;
	private final SectionCompressor.TripleFile sections;

	public CompressionResultFile(File triples, long tripleCount, SectionCompressor.TripleFile sections) throws IOException {
		this.triples = triples;
		this.tripleCount = tripleCount;
		this.subjects = new CompressNodeReader(sections.openRSubject());
		this.predicates = new CompressNodeReader(sections.openRPredicate());
		this.objects = new CompressNodeReader(sections.openRObject());
		this.sections = sections;
	}

	@Override
	public File getTriples() {
		return triples;
	}

	@Override
	public long getTripleCount() {
		return tripleCount;
	}

	@Override
	public ExceptionIterator<IndexedNode, IOException> getSubjects() {
		return subjects;
	}

	@Override
	public ExceptionIterator<IndexedNode, IOException> getPredicates() {
		return predicates;
	}

	@Override
	public ExceptionIterator<IndexedNode, IOException> getObjects() {
		return objects;
	}

	@Override
	public void delete() {
		sections.delete();
	}

	@Override
	public long getSubjectsCount() {
		return subjects.getSize();
	}

	@Override
	public long getPredicatesCount() {
		return predicates.getSize();
	}

	@Override
	public long getObjectsCount() {
		return objects.getSize();
	}

	@Override
	public long getSharedCount() {
		return tripleCount;
	}

	@Override
	public void close() throws IOException {
		try {
			try {
				this.objects.close();
			} finally {
				this.predicates.close();
			}
		} finally {
			this.subjects.close();
		}
	}
}
