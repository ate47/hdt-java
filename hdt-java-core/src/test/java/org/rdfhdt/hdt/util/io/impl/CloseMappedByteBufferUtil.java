package org.rdfhdt.hdt.util.io.impl;

/**
 * Utility class to trace unclosed mapped buffer.
 *
 * @author Antoine Willerval
 */
public class CloseMappedByteBufferUtil {
    /**
     * start recording map tests
     */
    public static void markMapTest() {
        CloseMappedByteBufferImpl.markMapTest();
    }

    /**
     * end recording of map tests and crash if a mapped element wasn't closed
     */
    public static void crashMapTest() {
        CloseMappedByteBufferImpl.crashMapTest();
    }
}
