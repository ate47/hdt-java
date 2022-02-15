package org.rdfhdt.hdt.dictionary.impl.utilDiff;

import org.rdfhdt.hdt.compact.bitmap.Bitmap;
import org.rdfhdt.hdt.dictionary.impl.utilCat.CatElement;

import java.util.ArrayList;
import java.util.Iterator;

public class DiffWrapper implements Iterator<CatElement> {
    public final Iterator<? extends CharSequence> sectionIter;
    public final Bitmap bitmap;
    public final String iterName;
    private CatElement next;
    private long count = 0;

    public DiffWrapper(Iterator<? extends CharSequence> sectionIter, Bitmap bitmap, String iterName) {
        this.sectionIter = sectionIter;
        this.bitmap = bitmap;
        this.iterName = iterName;
    }

    @Override
    public boolean hasNext() {

        while (sectionIter.hasNext()) {
            CharSequence element = sectionIter.next();
            if (bitmap.access(count)) {
                ArrayList<CatElement.IteratorPlusPosition> ids = new ArrayList<>();

                ids.add(new CatElement.IteratorPlusPosition(iterName, count + 1));
                next = new CatElement(element, ids);
                count++;
                return true;
            }
            count++;
        }
        return false;
    }

    @Override
    public CatElement next() {
        return next;
    }
}
