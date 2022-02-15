package org.rdfhdt.hdt.compact.bitmap;

import org.rdfhdt.hdt.listener.ProgressListener;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Class to create bitmaps
 *
 * @author ate47
 */
public abstract class BitmapFactory {
    /**
     * bitmap type plain
     */
    public static final byte TYPE_BITMAP_PLAIN = 1;

    private static BitmapFactory instance;

    private static BitmapFactory getInstance() {
        if (instance == null) {
            try {
                // Try to instantiate pro
                Class<?> managerImplClass = Class.forName("c");
                instance = (BitmapFactory) managerImplClass.newInstance();
            } catch (Exception e1) {
                try {
                    // Pro not found, instantiate normal
                    Class<?> managerImplClass = Class.forName("org.rdfhdt.hdt.compact.bitmap.BitmapFactoryImpl");
                    instance = (BitmapFactory) managerImplClass.newInstance();
                } catch (ClassNotFoundException e) {
                    throw new RuntimeException("Class org.rdfhdt.hdt.compact.bitmap.BitmapFactoryImpl not found. Did you include the HDT implementation jar?");
                } catch (InstantiationException e) {
                    throw new RuntimeException("Cannot create implementation for HDTManager. Does the class org.rdfhdt.hdt.compact.bitmap.BitmapFactoryImpl inherit from BitmapFactory?");
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return instance;
    }


    /**
     * create a unmodifiable empty bitmap, only the {@link Bitmap#access(long)}, {@link Bitmap#getNumBits()}
     * and {@link Bitmap#countZeros()} methods can be used, other methods behaviors aren't specified.
     *
     * @param size the bitmap size
     * @return bitmap
     */
    public static Bitmap createEmptyBitmap(long size) {
        return getInstance().doCreateEmptyBitmap(size);
    }

    /**
     * create a bitmap
     *
     * @param type the bitmap type
     * @return bitmap
     */
    public static Bitmap createBitmap(byte type) {
        return getInstance().doCreateBitmap(type);
    }

    /**
     * create a bitmap
     *
     * @param type the bitmap type
     * @return bitmap
     */
    public static Bitmap createBitmap(String type) {
        return getInstance().doCreateBitmap(type);
    }

    /**
     * Create a bitmap from a stream
     *
     * @param input Stream to read
     * @return bitmap
     * @throws IOException error with the stream
     */
    public static Bitmap createBitmap(InputStream input) throws IOException {
        return getInstance().doCreateBitmap(input);
    }

    /**
     * create a modifiable bitmap
     *
     * @param type the bitmap type
     * @return bitmap
     */
    public static ModifiableBitmap createModifiableBitmap(byte type) {
        return getInstance().doCreateModifiableBitmap(type);
    }

    /**
     * create a modifiable bitmap
     *
     * @param type the bitmap type
     * @return bitmap
     */
    public static ModifiableBitmap createModifiableBitmap(String type) {
        return getInstance().doCreateModifiableBitmap(type);
    }

    /**
     * Create a modifiable bitmap from a stream
     *
     * @param input Stream to read
     * @return bitmap
     */
    public static ModifiableBitmap createModifiableBitmap(InputStream input) throws IOException {
        return getInstance().doCreateModifiableBitmap(input);
    }

    /**
     * create a modifiable bitmap with only the read/write methods:
     * <p>
     * {@link ModifiableBitmap#access(long)}, {@link ModifiableBitmap#set(long, boolean)},
     * {@link ModifiableBitmap#append(boolean)}, {@link ModifiableBitmap#countZeros()},
     * {@link ModifiableBitmap#getNumBits()}, {@link ModifiableBitmap#load(InputStream, ProgressListener)},
     * {@link ModifiableBitmap#save(OutputStream, ProgressListener)}.
     *
     * @param size the bitmap size
     * @return bitmap
     */
    public static ModifiableBitmap createMemoryRWModifiableBitmap(long size) {
        return getInstance().doCreateMemoryRWModifiableBitmap(size);
    }


    // Abstract methods for the current implementation
    protected abstract Bitmap doCreateEmptyBitmap(long size);
    protected abstract Bitmap doCreateBitmap(byte type);
    protected abstract Bitmap doCreateBitmap(String type);
    protected abstract Bitmap doCreateBitmap(InputStream input) throws IOException;
    protected abstract ModifiableBitmap doCreateModifiableBitmap(byte type);
    protected abstract ModifiableBitmap doCreateModifiableBitmap(String type);
    protected abstract ModifiableBitmap doCreateModifiableBitmap(InputStream input) throws IOException;
    protected abstract ModifiableBitmap doCreateMemoryRWModifiableBitmap(long size);
}
