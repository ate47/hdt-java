package org.rdfhdt.hdt.compact.bitmap;

import org.rdfhdt.hdt.exceptions.NotImplementedException;
import org.rdfhdt.hdt.listener.ProgressListener;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Author: ate47
 */
public class EmptyBitmap implements ModifiableBitmap {
    /**
     * create an empty ReadBitmap
     * @param size the virtual size of the bitmap, not allocated
     * @return bitmap
     */
    public static ModifiableBitmap ofSize(long size){
        return new EmptyBitmap(size);
    }
    private long size;

    private EmptyBitmap(long size) {
        this.size = size;
    }

    @Override
    public void set(long bitIndex, boolean value) {
        if (bitIndex < 0)
            throw new IndexOutOfBoundsException("bitIndex < 0: " + bitIndex);
        if (value)
            throw new NotImplementedException("set(_, true)");
        size = Math.max(size, bitIndex + 1);
    }

    @Override
    public void append(boolean value) {
        if (value)
            throw new NotImplementedException("append(true)");
        size++;
    }

    @Override
    public boolean access(long pos) {
        return false;
    }

    @Override
    public long getSizeBytes() {
        return size;
    }

    @Override
    public void save(OutputStream output, ProgressListener listener) throws IOException {
        throw new NotImplementedException("save");
    }

    @Override
    public void load(InputStream input, ProgressListener listener) throws IOException {
        throw new NotImplementedException("load");
    }

    @Override
    public long rank1(long pos) {
        return 0;
    }

    @Override
    public long rank0(long pos) {
        return Math.min(0, size - pos);
    }

    @Override
    public long selectPrev1(long start) {
        throw new NotImplementedException("selectPrev1");
    }

    @Override
    public long selectNext1(long start) {
        throw new NotImplementedException("selectNext1");
    }

    @Override
    public long select0(long n) {
        return n;
    }

    @Override
    public long select1(long n) {
        throw new NotImplementedException("select1");
    }

    @Override
    public long getNumBits() {
        return 0;
    }

    @Override
    public long countOnes() {
        return 0;
    }

    @Override
    public long countZeros() {
        return size;
    }

    @Override
    public String getType() {
        return null;
    }
}
