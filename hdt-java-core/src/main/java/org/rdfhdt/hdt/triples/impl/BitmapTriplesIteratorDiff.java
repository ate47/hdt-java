package org.rdfhdt.hdt.triples.impl;

import org.rdfhdt.hdt.compact.bitmap.ModifiableBitmap;
import org.rdfhdt.hdt.exceptions.NotFoundException;
import org.rdfhdt.hdt.triples.IteratorTripleString;
import org.rdfhdt.hdt.triples.TripleString;
import org.rdfhdt.hdt.hdt.HDT;

public class BitmapTriplesIteratorDiff {
    private final HDT hdtOriginal;
    private final HDT hdtDiff;
    private final ModifiableBitmap bitmap;

    public BitmapTriplesIteratorDiff(HDT hdtOriginal, HDT hdtDiff, ModifiableBitmap bitmap) {
        this.hdtOriginal = hdtOriginal;
        this.hdtDiff = hdtDiff;
        this.bitmap = bitmap;
    }

    /**
     * fill the diff bitmap
     */
    public void fillBitmap() {
        IteratorTripleString iterator1;
        try {
            iterator1 = hdtOriginal.search("", "", "");
        } catch (NotFoundException e) {
            throw new RuntimeException(e);
        }
        long index = 0L;
        while (iterator1.hasNext()) {
            TripleString origNext = iterator1.next();
            try {
                IteratorTripleString otherTriple = hdtDiff.search(
                        origNext.getSubject(),
                        origNext.getPredicate(),
                        origNext.getObject()
                );
                if (otherTriple.hasNext()) {
                    bitmap.set(index, true);
                }
            } catch (NotFoundException e) {
                // continue
            }
            index++;
        }
    }
}
