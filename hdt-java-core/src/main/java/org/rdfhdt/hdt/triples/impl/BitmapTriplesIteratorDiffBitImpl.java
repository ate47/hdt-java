package org.rdfhdt.hdt.triples.impl;

import org.rdfhdt.hdt.compact.bitmap.Bitmap;
import org.rdfhdt.hdt.dictionary.FourSectionBitmap;
import org.rdfhdt.hdt.dictionary.impl.FourSectionBitmapImpl;
import org.rdfhdt.hdt.enums.ResultEstimationType;
import org.rdfhdt.hdt.enums.TripleComponentOrder;
import org.rdfhdt.hdt.exceptions.NotImplementedException;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.triples.BitmapTriplesIteratorDiffBit;
import org.rdfhdt.hdt.triples.IteratorTripleID;
import org.rdfhdt.hdt.triples.TripleID;

public class BitmapTriplesIteratorDiffBitImpl implements BitmapTriplesIteratorDiffBit {

    private final HDT hdtOriginal;
    private final FourSectionBitmap bitmaps;
    private final IteratorTripleID iterator;
    private final Bitmap bitmap;
    private TripleID next;
    private long count = 0;

    public BitmapTriplesIteratorDiffBitImpl(HDT hdtOriginal, Bitmap bitmap, IteratorTripleID iterator) {
        this.hdtOriginal = hdtOriginal;
        this.iterator = iterator;
        this.bitmap = bitmap;

        bitmaps = new FourSectionBitmapImpl(hdtOriginal.getDictionary());
    }

    @Override
    public boolean hasPrevious() {
        return false;
    }

    @Override
    public TripleID previous() {
        return null;
    }

    @Override
    public void goToStart() {

    }

    @Override
    public boolean canGoTo() {
        return false;
    }

    @Override
    public void goTo(long pos) {

    }

    @Override
    public long estimatedNumResults() {
        return hdtOriginal.getTriples().searchAll().estimatedNumResults();
    }

    @Override
    public ResultEstimationType numResultEstimation() {
        return null;
    }

    @Override
    public TripleComponentOrder getOrder() {
        return null;
    }

    @Override
    public long getLastTriplePosition() {
        throw new NotImplementedException();
    }

    @Override
    public boolean hasNext() {

        while (iterator.hasNext()) {
            TripleID tripleID = iterator.next();
            if (!this.bitmap.access(count)) { // triple not deleted bit = 0
                next = tripleID;
                long predId = next.getPredicate();
                long subjId = next.getSubject();
                long objId = next.getObject();
                // assign true for elements to keep when creating dictionary
                bitmaps.setSubjectBit(subjId, true);
                bitmaps.setPredicateBit(predId, true);
                bitmaps.setObjectBit(objId, true);
                count++;
                return true;
            }
            count++;
        }
        return false;
    }

    @Override
    public FourSectionBitmap getBitmaps() {
        return bitmaps;
    }

    public long getCount() {
        return count;
    }

    @Override
    public TripleID next() {
        return next;
    }
}
