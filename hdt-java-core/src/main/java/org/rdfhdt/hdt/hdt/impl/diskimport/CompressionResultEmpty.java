package org.rdfhdt.hdt.hdt.impl.diskimport;

import org.rdfhdt.hdt.iterator.utils.ExceptionIterator;
import org.rdfhdt.hdt.triples.IndexedNode;
import org.rdfhdt.hdt.util.io.compress.MappedId;

import java.io.IOException;

public class CompressionResultEmpty implements CompressionResult {
    @Override
    public long getTripleCount() {
        return 0;
    }

    @Override
    public ExceptionIterator<IndexedNode, IOException> getSubjects() {
        return ExceptionIterator.empty();
    }

    @Override
    public ExceptionIterator<IndexedNode, IOException> getPredicates() {
        return ExceptionIterator.empty();
    }

    @Override
    public ExceptionIterator<IndexedNode, IOException> getObjects() {
        return ExceptionIterator.empty();
    }

    @Override
    public ExceptionIterator<MappedId, IOException> getMappedIdSubjects() {
        return ExceptionIterator.empty();
    }

    @Override
    public ExceptionIterator<MappedId, IOException> getMappedIdPredicates() {
        return ExceptionIterator.empty();
    }

    @Override
    public ExceptionIterator<MappedId, IOException> getMappedIdObjects() {
        return ExceptionIterator.empty();
    }

    @Override
    public long getSubjectsCount() {
        return 0;
    }

    @Override
    public long getPredicatesCount() {
        return 0;
    }

    @Override
    public long getObjectsCount() {
        return 0;
    }

    @Override
    public long getSharedCount() {
        return 0;
    }

    @Override
    public void delete() throws IOException {
    }

    @Override
    public long getRawSize() {
        return 0;
    }

    @Override
    public void close() throws IOException {
    }
}
