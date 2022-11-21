package org.rdfhdt.hdt.util.io.impl;

import org.rdfhdt.hdt.util.io.CloseMappedByteBuffer;
import org.rdfhdt.hdt.util.io.IOUtil;

import java.io.Closeable;
import java.nio.*;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class CloseMappedByteBufferImpl implements CloseMappedByteBuffer {
	private static final AtomicLong ID_GEN = new AtomicLong();
	private static final Map<Long, Throwable> MAP_TEST_MAP = new HashMap<>();
	private static boolean mapTest = false;

	static void markMapTest() {
		mapTest = true;
		MAP_TEST_MAP.clear();
	}

	static void crashMapTest() {
		mapTest = false;
		if (!MAP_TEST_MAP.isEmpty()) {
			AssertionError re = new AssertionError(MAP_TEST_MAP.size() + " MAP(S) NOT CLOSED!");

			MAP_TEST_MAP.entrySet().stream()
					.sorted(Comparator.comparingLong(Map.Entry::getKey))
					.map(Map.Entry::getValue)
					.forEach(re::addSuppressed);

			throw re;
		}
	}

	private final long id = ID_GEN.incrementAndGet();
	private final ByteBuffer buffer;
	private final boolean duplicated;

	public CloseMappedByteBufferImpl(String filename, ByteBuffer buffer, boolean duplicated) {
		this.duplicated = duplicated;
		this.buffer = buffer;
		if (mapTest && !duplicated) {
			synchronized (MAP_TEST_MAP) {
				MAP_TEST_MAP.put(id, new Throwable("MAP " + filename + "#" + id + "|" + buffer));
			}
		}
	}

	@Override
	public void close() {
		if (mapTest && !duplicated) {
			synchronized (MAP_TEST_MAP) {
				MAP_TEST_MAP.remove(id);
			}
		}
		IOUtil.cleanBuffer(buffer);
	}

	@Override
	public boolean isLoaded() {
		return ((MappedByteBuffer) buffer).isLoaded();
	}

	@Override
	public MappedByteBuffer load() {
		return ((MappedByteBuffer) buffer).load();
	}

	@Override
	public MappedByteBuffer force() {
		return ((MappedByteBuffer) buffer).force();
	}

	@Override
	public ByteBuffer position(int newPosition) {
		return buffer.position(newPosition);
	}

	@Override
	public ByteBuffer limit(int newLimit) {
		return buffer.limit(newLimit);
	}

	@Override
	public ByteBuffer mark() {
		return buffer.mark();
	}

	@Override
	public ByteBuffer reset() {
		return buffer.reset();
	}

	@Override
	public ByteBuffer clear() {
		return buffer.clear();
	}

	@Override
	public ByteBuffer flip() {
		return buffer.flip();
	}

	@Override
	public ByteBuffer rewind() {
		return buffer.rewind();
	}

	@Override
	public ByteBuffer slice() {
		return buffer.slice();
	}

	@Override
	public ByteBuffer duplicate() {
		return buffer.duplicate();
	}

	@Override
	public ByteBuffer asReadOnlyBuffer() {
		return buffer.asReadOnlyBuffer();
	}

	@Override
	public byte get() {
		return buffer.get();
	}

	@Override
	public ByteBuffer put(byte b) {
		return buffer.put(b);
	}

	@Override
	public byte get(int index) {
		return buffer.get(index);
	}

	@Override
	public ByteBuffer put(int index, byte b) {
		return buffer.put(index, b);
	}

	@Override
	public ByteBuffer get(byte[] dst, int offset, int length) {
		return buffer.get(dst, offset, length);
	}

	@Override
	public ByteBuffer get(byte[] dst) {
		return buffer.get(dst);
	}

	@Override
	public ByteBuffer put(ByteBuffer src) {
		return buffer.put(src);
	}

	@Override
	public ByteBuffer put(byte[] src, int offset, int length) {
		return buffer.put(src, offset, length);
	}

	@Override
	public ByteBuffer put(byte[] src) {
		return buffer.put(src);
	}

	@Override
	public boolean hasArray() {
		return buffer.hasArray();
	}

	@Override
	public byte[] array() {
		return buffer.array();
	}

	@Override
	public int arrayOffset() {
		return buffer.arrayOffset();
	}

	@Override
	public ByteBuffer compact() {
		return buffer.compact();
	}

	@Override
	public boolean isDirect() {
		return buffer.isDirect();
	}

	@Override
	public int compareTo(ByteBuffer that) {
		return buffer.compareTo(that);
	}

	@Override
	public int mismatch(ByteBuffer that) {
		return buffer.mismatch(that);
	}

	@Override
	public ByteOrder order() {
		return buffer.order();
	}

	@Override
	public ByteBuffer order(ByteOrder bo) {
		return buffer.order(bo);
	}

	@Override
	public int alignmentOffset(int index, int unitSize) {
		return buffer.alignmentOffset(index, unitSize);
	}

	@Override
	public ByteBuffer alignedSlice(int unitSize) {
		return buffer.alignedSlice(unitSize);
	}

	@Override
	public char getChar() {
		return buffer.getChar();
	}

	@Override
	public ByteBuffer putChar(char value) {
		return buffer.putChar(value);
	}

	@Override
	public char getChar(int index) {
		return buffer.getChar(index);
	}

	@Override
	public ByteBuffer putChar(int index, char value) {
		return buffer.putChar(index, value);
	}

	@Override
	public CharBuffer asCharBuffer() {
		return buffer.asCharBuffer();
	}

	@Override
	public short getShort() {
		return buffer.getShort();
	}

	@Override
	public ByteBuffer putShort(short value) {
		return buffer.putShort(value);
	}

	@Override
	public short getShort(int index) {
		return buffer.getShort(index);
	}

	@Override
	public ByteBuffer putShort(int index, short value) {
		return buffer.putShort(index, value);
	}

	@Override
	public ShortBuffer asShortBuffer() {
		return buffer.asShortBuffer();
	}

	@Override
	public int getInt() {
		return buffer.getInt();
	}

	@Override
	public ByteBuffer putInt(int value) {
		return buffer.putInt(value);
	}

	@Override
	public int getInt(int index) {
		return buffer.getInt(index);
	}

	@Override
	public ByteBuffer putInt(int index, int value) {
		return buffer.putInt(index, value);
	}

	@Override
	public IntBuffer asIntBuffer() {
		return buffer.asIntBuffer();
	}

	@Override
	public long getLong() {
		return buffer.getLong();
	}

	@Override
	public ByteBuffer putLong(long value) {
		return buffer.putLong(value);
	}

	@Override
	public long getLong(int index) {
		return buffer.getLong(index);
	}

	@Override
	public ByteBuffer putLong(int index, long value) {
		return buffer.putLong(index, value);
	}

	@Override
	public LongBuffer asLongBuffer() {
		return buffer.asLongBuffer();
	}

	@Override
	public float getFloat() {
		return buffer.getFloat();
	}

	@Override
	public ByteBuffer putFloat(float value) {
		return buffer.putFloat(value);
	}

	@Override
	public float getFloat(int index) {
		return buffer.getFloat(index);
	}

	@Override
	public ByteBuffer putFloat(int index, float value) {
		return buffer.putFloat(index, value);
	}

	@Override
	public FloatBuffer asFloatBuffer() {
		return buffer.asFloatBuffer();
	}

	@Override
	public double getDouble() {
		return buffer.getDouble();
	}

	@Override
	public ByteBuffer putDouble(double value) {
		return buffer.putDouble(value);
	}

	@Override
	public double getDouble(int index) {
		return buffer.getDouble(index);
	}

	@Override
	public ByteBuffer putDouble(int index, double value) {
		return buffer.putDouble(index, value);
	}

	@Override
	public DoubleBuffer asDoubleBuffer() {
		return buffer.asDoubleBuffer();
	}

	@Override
	public int capacity() {
		return buffer.capacity();
	}

	@Override
	public int position() {
		return buffer.position();
	}

	@Override
	public int limit() {
		return buffer.limit();
	}

	@Override
	public int remaining() {
		return buffer.remaining();
	}

	@Override
	public boolean hasRemaining() {
		return buffer.hasRemaining();
	}

	@Override
	public boolean isReadOnly() {
		return buffer.isReadOnly();
	}

	@Override
	public ByteBuffer getInternalBuffer() {
		return buffer;
	}
}
