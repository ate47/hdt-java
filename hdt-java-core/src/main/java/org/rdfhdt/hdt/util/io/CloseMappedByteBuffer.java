package org.rdfhdt.hdt.util.io;

import java.io.Closeable;
import java.nio.*;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public interface CloseMappedByteBuffer extends Closeable {
	boolean isLoaded();

	MappedByteBuffer load();

	MappedByteBuffer force();

	ByteBuffer position(int newPosition);

	ByteBuffer limit(int newLimit);

	ByteBuffer mark();

	ByteBuffer reset();

	ByteBuffer clear();

	ByteBuffer flip();

	ByteBuffer rewind();

	ByteBuffer slice();

	ByteBuffer duplicate();

	ByteBuffer asReadOnlyBuffer();

	byte get();

	ByteBuffer put(byte b);

	byte get(int index);

	ByteBuffer put(int index, byte b);

	ByteBuffer get(byte[] dst, int offset, int length);

	ByteBuffer get(byte[] dst);

	ByteBuffer put(ByteBuffer src);

	ByteBuffer put(byte[] src, int offset, int length);

	ByteBuffer put(byte[] src);

	boolean hasArray();

	byte[] array();

	int arrayOffset();

	ByteBuffer compact();

	boolean isDirect();

	int compareTo(ByteBuffer that);

	int mismatch(ByteBuffer that);

	ByteOrder order();

	ByteBuffer order(ByteOrder bo);

	int alignmentOffset(int index, int unitSize);

	ByteBuffer alignedSlice(int unitSize);

	char getChar();

	ByteBuffer putChar(char value);

	char getChar(int index);

	ByteBuffer putChar(int index, char value);

	CharBuffer asCharBuffer();

	short getShort();

	ByteBuffer putShort(short value);

	short getShort(int index);

	ByteBuffer putShort(int index, short value);

	ShortBuffer asShortBuffer();

	int getInt();

	ByteBuffer putInt(int value);

	int getInt(int index);

	ByteBuffer putInt(int index, int value);

	IntBuffer asIntBuffer();

	long getLong();

	ByteBuffer putLong(long value);

	long getLong(int index);

	ByteBuffer putLong(int index, long value);

	LongBuffer asLongBuffer();

	float getFloat();

	ByteBuffer putFloat(float value);

	float getFloat(int index);

	ByteBuffer putFloat(int index, float value);

	FloatBuffer asFloatBuffer();

	double getDouble();

	ByteBuffer putDouble(double value);

	double getDouble(int index);

	ByteBuffer putDouble(int index, double value);

	DoubleBuffer asDoubleBuffer();

	int capacity();

	int position();

	int limit();

	int remaining();

	boolean hasRemaining();

	boolean isReadOnly();

	ByteBuffer getInternalBuffer();
}
