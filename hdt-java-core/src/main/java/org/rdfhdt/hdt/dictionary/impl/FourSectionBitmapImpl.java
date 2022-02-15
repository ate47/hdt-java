package org.rdfhdt.hdt.dictionary.impl;

import org.rdfhdt.hdt.compact.bitmap.Bitmap;
import org.rdfhdt.hdt.compact.bitmap.Bitmap64;
import org.rdfhdt.hdt.compact.bitmap.BitmapFactory;
import org.rdfhdt.hdt.compact.bitmap.ModifiableBitmap;
import org.rdfhdt.hdt.dictionary.Dictionary;
import org.rdfhdt.hdt.dictionary.FourSectionBitmap;

public class FourSectionBitmapImpl implements FourSectionBitmap {

    private final long numShared;
    private final ModifiableBitmap predicatesBitmap;
    private final ModifiableBitmap subjectsBitmap;
    private final ModifiableBitmap objectBitmap;
    private final ModifiableBitmap sharedSubjectBitmap;
    private final ModifiableBitmap sharedObjectBitmap;

    public FourSectionBitmapImpl(Dictionary dictionary) {
        numShared = dictionary.getNshared();
        predicatesBitmap = BitmapFactory.createMemoryRWModifiableBitmap(dictionary.getNpredicates());
        sharedSubjectBitmap = BitmapFactory.createMemoryRWModifiableBitmap(numShared);
        sharedObjectBitmap = BitmapFactory.createMemoryRWModifiableBitmap(numShared);
        subjectsBitmap = BitmapFactory.createMemoryRWModifiableBitmap(dictionary.getNsubjects() - numShared);
        objectBitmap = BitmapFactory.createMemoryRWModifiableBitmap(dictionary.getNobjects() - numShared);
    }

    @Override
    public ModifiableBitmap getPredicatesBitmap() {
        return predicatesBitmap;
    }

    @Override
    public ModifiableBitmap getSubjectsBitmap() {
        return subjectsBitmap;
    }

    @Override
    public ModifiableBitmap getSharedObjectBitmap() {
        return sharedObjectBitmap;
    }

    @Override
    public ModifiableBitmap getSharedSubjectBitmap() {
        return sharedSubjectBitmap;
    }

    @Override
    public ModifiableBitmap getObjectBitmap() {
        return objectBitmap;
    }

    @Override
    public void setSubjectBit(long id, boolean value) {
        if (id <= numShared) {
            sharedSubjectBitmap.set(id - 1, value);
        } else {
            subjectsBitmap.set(id - numShared - 1, value);
        }
    }

    @Override
    public void setPredicateBit(long id, boolean value) {
        predicatesBitmap.set(id - 1, value);
    }

    @Override
    public void setObjectBit(long id, boolean value) {
        if (id <= numShared) {
            sharedObjectBitmap.set(id - 1, value);
        } else {
            objectBitmap.set(id - numShared - 1, value);
        }
    }

    @Override
    public boolean getSubjectBit(long id) {
        if (id <= numShared) {
            return sharedSubjectBitmap.access(id - 1);
        } else {
            return subjectsBitmap.access(id - numShared - 1);
        }
    }

    @Override
    public boolean getPredicateBit(long id) {
        return predicatesBitmap.access(id - 1);
    }

    @Override
    public boolean getObjectBit(long id) {
        if (id <= numShared) {
            return sharedObjectBitmap.access(id - 1);
        } else {
            return objectBitmap.access(id - numShared - 1);
        }
    }
}
