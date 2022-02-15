package org.rdfhdt.hdt.triples;

import org.rdfhdt.hdt.dictionary.FourSectionBitmap;

public interface BitmapTriplesIteratorDiffBit extends IteratorTripleID {
    /**
     * @return the count of the iterator
     */
    long getCount();

    /**
     * @return the bitmaps
     */
    FourSectionBitmap getBitmaps();

    /**
     * consume all the next()
     */
    default void consume() {
        while (hasNext())
            next();
    }
}
