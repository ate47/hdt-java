package org.rdfhdt.hdt.hdt.impl.diskimport;

import org.rdfhdt.hdt.iterator.utils.ExceptionIterator;
import org.rdfhdt.hdt.triples.IndexedNode;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

/**
 * Result for the {@link org.rdfhdt.hdt.hdt.impl.diskimport.SectionCompressor}
 * @author Antoine Willerval
 */
public interface CompressionResult extends Closeable {
	/**
	 * partial mode for config
	 * @see org.rdfhdt.hdt.hdt.impl.diskimport.CompressionResultPartial
	 */
	String COMPRESSION_MODE_PARTIAL = "compressionPartial";
	/**
	 * complete mode for config
	 * @see org.rdfhdt.hdt.hdt.impl.diskimport.CompressionResultFile
	 */
	String COMPRESSION_MODE_COMPLETE = "compressionComplete";

	/**
	 * @return the triples file
	 */
	File getTriples();
	/**
	 * @return the number of triple
	 */
	long getTripleCount();
	/**
	 * @return a sorted iterator of subject
	 */
	ExceptionIterator<IndexedNode, IOException> getSubjects();
	/**
	 * @return a sorted iterator of predicates
	 */
	ExceptionIterator<IndexedNode, IOException> getPredicates();
	/**
	 * @return a sorted iterator of objects
	 */
	ExceptionIterator<IndexedNode, IOException> getObjects();
	/**
	 * @return the count of subjects
	 */
	long getSubjectsCount();
	/**
	 * @return the count of predicates
	 */
	long getPredicatesCount();
	/**
	 * @return the count of objects
	 */
	long getObjectsCount();
	/**
	 * @return the count of shared
	 */
	long getSharedCount();

	/**
	 * delete data associated with this result
	 */
	void delete();
}
