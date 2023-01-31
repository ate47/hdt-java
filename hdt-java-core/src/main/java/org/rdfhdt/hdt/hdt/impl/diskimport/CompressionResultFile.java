package org.rdfhdt.hdt.hdt.impl.diskimport;

import org.rdfhdt.hdt.iterator.utils.ExceptionIterator;
import org.rdfhdt.hdt.triples.IndexedNode;
import org.rdfhdt.hdt.util.io.IOUtil;
import org.rdfhdt.hdt.util.io.compress.CompressNodeReader;
import org.rdfhdt.hdt.util.io.compress.MappedElementReader;
import org.rdfhdt.hdt.util.io.compress.MappedId;

import java.io.IOException;

/**
 * Implementation of {@link org.rdfhdt.hdt.hdt.impl.diskimport.CompressionResult} for full file reading
 * @author Antoine Willerval
 */
public class CompressionResultFile implements CompressionResult {
	private final long tripleCount;
	private final long ntRawSize;
	private final CompressNodeReader subjects;
	private final CompressNodeReader predicates;
	private final CompressNodeReader objects;
	private final MappedElementReader mappedSubjects;
	private final MappedElementReader mappedPredicates;
	private final MappedElementReader mappedObjects;
	private boolean closed;
	private final SectionCompressor.TripleFile sections;

	public CompressionResultFile(long tripleCount, long ntRawSize, SectionCompressor.TripleFile sections) throws IOException {
		this.tripleCount = tripleCount;
		this.ntRawSize = ntRawSize;
		this.subjects = new CompressNodeReader(sections.openRSubject());
		this.predicates = new CompressNodeReader(sections.openRPredicate());
		this.objects = new CompressNodeReader(sections.openRObject());
		this.mappedSubjects = new MappedElementReader(sections.openRMappingSubject());
		this.mappedPredicates = new MappedElementReader(sections.openRMappingPredicate());
		this.mappedObjects = new MappedElementReader(sections.openRMappingObject());

		this.sections = sections;
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
	public ExceptionIterator<MappedId, IOException> getMappedIdSubjects() {
		return mappedSubjects;
	}

	@Override
	public ExceptionIterator<MappedId, IOException> getMappedIdPredicates() {
		return mappedPredicates;
	}

	@Override
	public ExceptionIterator<MappedId, IOException> getMappedIdObjects() {
		return mappedObjects;
	}

	@Override
	public void delete() throws IOException {
		close();
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
	public long getRawSize() {
		return ntRawSize;
	}

	@Override
	public void close() throws IOException {
		if (closed) {
			return;
		}
		closed = true;
		IOUtil.closeAll(
				objects,
				predicates,
				subjects,
				mappedSubjects,
				mappedPredicates,
				mappedObjects
		);
	}
}
