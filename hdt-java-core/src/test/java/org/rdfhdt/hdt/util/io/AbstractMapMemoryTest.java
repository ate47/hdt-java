package org.rdfhdt.hdt.util.io;

import org.junit.After;
import org.junit.Before;
import org.rdfhdt.hdt.util.io.impl.CloseMappedByteBufferUtil;

/**
 * extend this class to all test using mapped memory to check if no unclose mapped file are present.
 * <p>
 * This code is equivalent to add:
 *
 * <pre>
 * {@literal @Before}
 *  public void setup() {
 *      CloseMappedByteBufferUtil.markMapTest();
 *  }
 *
 * {@literal @After}
 *  public void complete() {
 *      CloseMappedByteBufferUtil.crashMapTest();
 *  }
 * </pre>
 *
 * @author Antoine Willerval
 */
public class AbstractMapMemoryTest {
    @Before
    public void setup() {
        CloseMappedByteBufferUtil.markMapTest();
    }

    @After
    public void complete() {
        CloseMappedByteBufferUtil.crashMapTest();
    }
}
