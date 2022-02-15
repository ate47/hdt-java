package org.rdfhdt.hdt.dictionary;

import org.rdfhdt.hdt.dictionary.impl.utilCat.CatMapping;
import org.rdfhdt.hdt.listener.ProgressListener;

import java.util.HashMap;

public interface DictionaryDiff {
    void diff(Dictionary dictionary, FourSectionBitmap bitmaps, ProgressListener listener);
    CatMapping getMappingBack();
    long getNumShared();
    HashMap<String, CatMapping> getAllMappings();
}
