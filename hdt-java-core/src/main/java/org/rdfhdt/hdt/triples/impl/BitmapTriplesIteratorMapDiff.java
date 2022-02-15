package org.rdfhdt.hdt.triples.impl;


import org.rdfhdt.hdt.compact.bitmap.Bitmap;
import org.rdfhdt.hdt.dictionary.DictionaryDiff;
import org.rdfhdt.hdt.dictionary.impl.utilCat.CatMapping;
import org.rdfhdt.hdt.enums.ResultEstimationType;
import org.rdfhdt.hdt.enums.TripleComponentOrder;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.triples.IteratorTripleID;
import org.rdfhdt.hdt.triples.TripleID;
import org.rdfhdt.hdt.triples.TripleIDComparator;
import org.rdfhdt.hdt.triples.Triples;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


public class BitmapTriplesIteratorMapDiff implements IteratorTripleID {
    private final CatMapping subjMapping;
    private final CatMapping objMapping;
    private final CatMapping predMapping;
    private final CatMapping sharedMapping;
    private final long countTriples;
    private final DictionaryDiff dictionaryDiff;
    private final Triples triples;
    private final TripleIDComparator tripleIDComparator = new TripleIDComparator(TripleComponentOrder.SPO);
    private final Bitmap bitmap;
    private Iterator<TripleID> iterator;
    private long count = 0L;

    public BitmapTriplesIteratorMapDiff(HDT hdtOriginal, Bitmap bitmap, DictionaryDiff dictionaryDiff, long countTriples) {
        this.subjMapping = dictionaryDiff.getAllMappings().get("subject");
        this.objMapping = dictionaryDiff.getAllMappings().get("object");
        this.predMapping = dictionaryDiff.getAllMappings().get("predicate");
        this.sharedMapping = dictionaryDiff.getAllMappings().get("shared");
        this.dictionaryDiff = dictionaryDiff;
        this.countTriples = countTriples;
        this.triples = hdtOriginal.getTriples();
        this.bitmap = bitmap;
        this.iterator = getTripleID(0).iterator();
        this.count++;
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
        return countTriples;
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
    public boolean hasNext() {
        return count < dictionaryDiff.getMappingBack().getSize() || iterator.hasNext();
    }

    @Override
    public TripleID next() {
        if (!iterator.hasNext()) {
            iterator = getTripleID(count).iterator();
            count++;
        }
        return iterator.next();
    }

    private List<TripleID> getTripleID(long count) {
        List<TripleID> newTriples = new ArrayList<>();
        if (dictionaryDiff.getMappingBack().getSize() > 0) {
            long mapping = dictionaryDiff.getMappingBack().getMapping(count);
            IteratorTripleID it = this.triples.search(new TripleID(mapping, 0, 0));
            long index = 0L;
            while (it.hasNext()) {
                TripleID next = it.next();
                if (!bitmap.access(index)) {
                    newTriples.add(mapTriple(next));
                }
                index++;
            }
        }
        newTriples.sort(tripleIDComparator);
        return newTriples;
    }

    public TripleID mapTriple(TripleID tripleID) {

        long subjOld = tripleID.getSubject();
        long numShared = this.sharedMapping.getSize();
        long newSubjId;
        if (subjOld <= numShared) {
            if (this.sharedMapping.getType(subjOld - 1) == 1) {
                newSubjId = this.sharedMapping.getMapping(subjOld - 1);
            } else {
                newSubjId = this.sharedMapping.getMapping(subjOld - 1) + this.dictionaryDiff.getNumShared();
            }
        } else {
            if (this.subjMapping.getType(subjOld - numShared - 1) == 1)
                newSubjId = this.subjMapping.getMapping(subjOld - numShared - 1);
            else
                newSubjId = this.subjMapping.getMapping(subjOld - numShared - 1) + this.dictionaryDiff.getNumShared();
        }
        long newPredId = this.predMapping.getMapping(tripleID.getPredicate() - 1);

        long objOld = tripleID.getObject();
        long newObjId;
        if (objOld <= numShared) {
            long type = this.sharedMapping.getType(objOld - 1);
            if (type == 1) {
                newObjId = this.sharedMapping.getMapping(objOld - 1);
            } else {
                newObjId = this.sharedMapping.getMapping(objOld - 1) + this.dictionaryDiff.getNumShared();
            }

        } else {
            if (this.objMapping.getType(objOld - numShared - 1) == 1)
                newObjId = this.objMapping.getMapping(objOld - numShared - 1);
            else
                newObjId = this.objMapping.getMapping(objOld - numShared - 1) + this.dictionaryDiff.getNumShared();
        }
        return new TripleID(newSubjId, newPredId, newObjId);
    }
}
