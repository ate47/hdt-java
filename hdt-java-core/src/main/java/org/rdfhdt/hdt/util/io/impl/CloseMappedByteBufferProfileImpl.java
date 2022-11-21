package org.rdfhdt.hdt.util.io.impl;

import org.rdfhdt.hdt.util.Profiler;

import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

public class CloseMappedByteBufferProfileImpl extends CloseMappedByteBufferImpl {
	private final Profiler profiler;

	public CloseMappedByteBufferProfileImpl(String filename, ByteBuffer buffer, boolean duplicated, Profiler profiler) {
		super(filename, buffer, duplicated);
		this.profiler = profiler;
	}

	@Override
	public MappedByteBuffer force() {
		return profiler.runWrite((s) -> super.force());
	}

	@Override
	public ByteBuffer position(int newPosition) {
		return profiler.runWrite((s) -> super.position(newPosition));
	}

	@Override
	public ByteBuffer limit(int newLimit) {
		return profiler.runWrite((s) -> super.limit(newLimit));
	}

	@Override
	public ByteBuffer mark() {
		return profiler.runWrite((s) -> super.mark());
	}

	@Override
	public ByteBuffer reset() {
		return profiler.runWrite((s) -> super.reset());
	}

	@Override
	public ByteBuffer clear() {
		return profiler.runWrite((s) -> super.clear());
	}

	@Override
	public ByteBuffer flip() {
		return profiler.runWrite((s) -> super.flip());
	}

	@Override
	public ByteBuffer rewind() {
		return profiler.runWrite((s) -> super.rewind());
	}

	@Override
	public ByteBuffer duplicate() {
		return profiler.runWrite((s) -> super.duplicate());
	}

	@Override
	public byte get() {
		return profiler.runRead((s) -> {
			s.set(Byte.SIZE);
			return super.get();
		});
	}

	@Override
	public ByteBuffer put(byte b) {
		return profiler.runWrite((s) -> {
			s.set(Byte.SIZE);
			return super.put(b);
		});
	}

	@Override
	public byte get(int index) {
		return profiler.runRead((s) -> {
			s.set(Byte.SIZE);
			return super.get(index);
		});
	}

	@Override
	public ByteBuffer put(int index, byte b) {
		return profiler.runWrite((s) -> {
			s.set(Byte.SIZE);
			return super.put(index, b);
		});
	}

	@Override
	public ByteBuffer get(byte[] dst, int offset, int length) {
		return profiler.runRead((s) -> {
			s.set((long) Byte.SIZE * length);
			return super.get(dst, offset, length);
		});
	}

	@Override
	public ByteBuffer get(byte[] dst) {
		return profiler.runRead((s) -> {
			s.set((long) Byte.SIZE * dst.length);
			return super.get(dst);
		});
	}

	@Override
	public ByteBuffer put(ByteBuffer src) {
		return profiler.runWrite((s) -> {
			s.set((long) Byte.SIZE * src.capacity());
			return super.put(src);
		});
	}

	@Override
	public ByteBuffer put(byte[] src, int offset, int length) {
		return profiler.runWrite((s) -> {
			s.set((long) Byte.SIZE * length);
			return super.put(src, offset, length);
		});
	}

	@Override
	public ByteBuffer put(byte[] src) {
		return profiler.runWrite((s) -> {
			s.set((long) Byte.SIZE * src.length);
			return super.put(src);
		});
	}

	@Override
	public byte[] array() {
		return profiler.runRead((s) -> {
			byte[] arr = super.array();
			s.set((long) Byte.SIZE * arr.length);
			return arr;
		});
	}

	@Override
	public int compareTo(ByteBuffer that) {
		return profiler.runRead((s) -> super.compareTo(that));
	}

	@Override
	public char getChar() {
		return profiler.runRead((s) -> {
			s.set(Character.SIZE);
			return super.getChar();
		});
	}

	@Override
	public ByteBuffer putChar(char value) {
		return profiler.runWrite((s) -> {
			s.set(Character.SIZE);
			return super.putChar(value);
		});
	}

	@Override
	public char getChar(int index) {
		return profiler.runRead((s) -> {
			s.set(Character.SIZE);
			return super.getChar(index);
		});
	}

	@Override
	public ByteBuffer putChar(int index, char value) {
		return profiler.runWrite((s) -> {
			s.set(Character.SIZE);
			return super.putChar(index, value);
		});
	}

	@Override
	public short getShort() {
		return profiler.runRead((s) -> {
			s.set(Short.SIZE);
			return super.getShort();
		});
	}

	@Override
	public ByteBuffer putShort(short value) {
		return profiler.runWrite((s) -> {
			s.set(Short.SIZE);
			return super.putShort(value);
		});
	}

	@Override
	public short getShort(int index) {
		return profiler.runRead((s) -> {
			s.set(Short.SIZE);
			return super.getShort(index);
		});
	}

	@Override
	public ByteBuffer putShort(int index, short value) {
		return profiler.runWrite((s) -> {
			s.set(Short.SIZE);
			return super.putShort(index, value);
		});
	}

	@Override
	public int getInt() {
		return profiler.runRead((s) -> {
			s.set(Integer.SIZE);
			return super.getInt();
		});
	}

	@Override
	public ByteBuffer putInt(int value) {
		return profiler.runWrite((s) -> {
			s.set(Integer.SIZE);
			return super.putInt(value);
		});
	}

	@Override
	public int getInt(int index) {
		return profiler.runRead((s) -> {
			s.set(Integer.SIZE);
			return super.getInt(index);
		});
	}

	@Override
	public ByteBuffer putInt(int index, int value) {
		return profiler.runWrite((s) -> {
			s.set(Integer.SIZE);
			return super.putInt(index, value);
		});
	}

	@Override
	public long getLong() {
		return profiler.runRead((s) -> {
			s.set(Long.SIZE);
			return super.getLong();
		});
	}

	@Override
	public ByteBuffer putLong(long value) {
		return profiler.runWrite((s) -> {
			s.set(Long.SIZE);
			return super.putLong(value);
		});
	}

	@Override
	public long getLong(int index) {
		return profiler.runRead((s) -> {
			s.set(Long.SIZE);
			return super.getLong(index);
		});
	}

	@Override
	public ByteBuffer putLong(int index, long value) {
		return profiler.runWrite((s) -> {
			s.set(Long.SIZE);
			return super.putLong(index, value);
		});
	}

	@Override
	public float getFloat() {
		return profiler.runRead((s) -> {
			s.set(Float.SIZE);
			return super.getFloat();
		});
	}

	@Override
	public ByteBuffer putFloat(float value) {
		return profiler.runWrite((s) -> {
			s.set(Float.SIZE);
			return super.putFloat(value);
		});
	}

	@Override
	public float getFloat(int index) {
		return profiler.runRead((s) -> {
			s.set(Float.SIZE);
			return super.getFloat(index);
		});
	}

	@Override
	public ByteBuffer putFloat(int index, float value) {
		return profiler.runWrite((s) -> {
			s.set(Float.SIZE);
			return super.putFloat(index, value);
		});
	}

	@Override
	public double getDouble() {
		return profiler.runRead((s) -> {
			s.set(Double.SIZE);
			return super.getDouble();
		});
	}

	@Override
	public ByteBuffer putDouble(double value) {
		return profiler.runWrite((s) -> {
			s.set(Double.SIZE);
			return super.putDouble(value);
		});
	}

	@Override
	public double getDouble(int index) {
		return profiler.runRead((s) -> {
			s.set(Double.SIZE);
			return super.getDouble(index);
		});
	}

	@Override
	public ByteBuffer putDouble(int index, double value) {
		return profiler.runWrite((s) -> {
			s.set(Double.SIZE);
			return super.putDouble(index, value);
		});
	}
}
