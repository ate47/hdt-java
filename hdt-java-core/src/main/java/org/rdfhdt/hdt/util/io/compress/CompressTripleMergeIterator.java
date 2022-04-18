package org.rdfhdt.hdt.util.io.compress;

import org.rdfhdt.hdt.enums.TripleComponentOrder;
import org.rdfhdt.hdt.iterator.utils.ExceptionIterator;
import org.rdfhdt.hdt.iterator.utils.MergeExceptionIterator;
import org.rdfhdt.hdt.triples.TripleID;
import org.rdfhdt.hdt.triples.TripleIDComparator;

import java.io.IOException;

/**
 * Version of {@link org.rdfhdt.hdt.iterator.utils.MergeExceptionIterator} with {@link org.rdfhdt.hdt.triples.TripleID}
 * @author Antoine Willerval
 */
public class CompressTripleMergeIterator extends MergeExceptionIterator<TripleID, IOException> {

	public CompressTripleMergeIterator(ExceptionIterator<TripleID, IOException> in1, ExceptionIterator<TripleID, IOException> in2, TripleComponentOrder order) {
		super(in1, in2, TripleIDComparator.getComparator(order));
	}
}