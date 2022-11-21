package org.rdfhdt.hdt.util.io.impl;

import org.rdfhdt.hdt.util.Profiler;

import java.io.IOException;
import java.io.InputStream;

public class ProfileInputStream extends InputStream {
	private final InputStream wrapper;
	private final Profiler profiler;

	public ProfileInputStream(InputStream wrapper, Profiler profiler) {
		this.wrapper = wrapper;
		this.profiler = profiler;
	}

	@Override
	public int read() throws IOException {
		return profiler.runRead((s) -> {
			s.set(Byte.SIZE);
			return wrapper.read();
		});
	}

	@Override
	public int read(byte[] b) throws IOException {
		return profiler.runRead((s) -> {
			int read = wrapper.read(b);
			s.set((long) Byte.SIZE * read);
			return read;
		});
	}

	@Override
	public int read(byte[] b, int off, int len) throws IOException {
		return profiler.runRead((s) -> {
			int read = wrapper.read(b, off, len);
			s.set((long) Byte.SIZE * read);
			return read;
		});
	}

	@Override
	public byte[] readAllBytes() throws IOException {
		return profiler.runRead((s) -> {
			byte[] bytes = wrapper.readAllBytes();
			s.set((long) Byte.SIZE * bytes.length);
			return bytes;
		});
	}

	@Override
	public byte[] readNBytes(int len) throws IOException {
		return profiler.runRead((s) -> {
			byte[] bytes = wrapper.readNBytes(len);
			s.set((long) Byte.SIZE * bytes.length);
			return bytes;
		});
	}

	@Override
	public int readNBytes(byte[] b, int off, int len) throws IOException {
		return profiler.runRead((s) -> {
			int bytes = wrapper.readNBytes(b, off, len);
			s.set((long) Byte.SIZE * bytes);
			return bytes;
		});
	}

	@Override
	public long skip(long n) throws IOException {
		return profiler.runRead((s) -> {
			long bytes = wrapper.skip(n);
			s.set(Byte.SIZE * bytes);
			return bytes;
		});
	}

	@Override
	public int available() throws IOException {
		return wrapper.available();
	}

	@Override
	public void close() throws IOException {
		wrapper.close();
	}

	@Override
	public void mark(int readlimit) {
		wrapper.mark(readlimit);
	}

	@Override
	public void reset() throws IOException {
		wrapper.reset();
	}

	@Override
	public boolean markSupported() {
		return wrapper.markSupported();
	}

	public Profiler getProfiler() {
		return profiler;
	}
}
