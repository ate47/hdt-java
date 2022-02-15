package org.rdfhdt.hdt.dictionary;

import org.rdfhdt.hdt.compact.bitmap.ModifiableBitmap;

public interface FourSectionBitmap {
    /**
     * @return the predicates bitmap
     */
    ModifiableBitmap getPredicatesBitmap();
    /**
     * @return the non shared subjects bitmap
     */
    ModifiableBitmap getSubjectsBitmap();
    /**
     * @return the shared subjects bitmap
     */
    ModifiableBitmap getSharedSubjectBitmap();
    /**
     * @return the shared objects bitmap
     */
    ModifiableBitmap getSharedObjectBitmap();
    /**
     * @return the non shared objects bitmap
     */
    ModifiableBitmap getObjectBitmap();

    /**
     * set a subject bit inside the right bitmap
     * @param id the subject id
     * @param value the bit value to set
     */
    void setSubjectBit(long id, boolean value);
    /**
     * set a predicate bit inside the right bitmap
     * @param id the subject id
     * @param value the bit value to set
     */
    void setPredicateBit(long id, boolean value);
    /**
     * set a object bit inside the right bitmap
     * @param id the subject id
     * @param value the bit value to set
     */
    void setObjectBit(long id, boolean value);

    /**
     * get a subject bit inside the right bitmap
     * @param id the subject id
     * @return the bit value
     */
    boolean getSubjectBit(long id);
    /**
     * get a predicate bit inside the right bitmap
     * @param id the subject id
     * @return the bit value
     */
    boolean getPredicateBit(long id);
    /**
     * get a object bit inside the right bitmap
     * @param id the subject id
     * @return the bit value
     */
    boolean getObjectBit(long id);
}
