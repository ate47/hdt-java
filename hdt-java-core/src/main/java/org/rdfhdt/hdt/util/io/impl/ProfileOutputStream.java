package org.rdfhdt.hdt.util.io.impl;

import org.rdfhdt.hdt.util.Profiler;

import java.io.IOException;
import java.io.OutputStream;

public class ProfileOutputStream extends OutputStream {
	private final OutputStream wrapper;
	private final Profiler profiler;

	public ProfileOutputStream(OutputStream wrapper, Profiler profiler) {
		this.wrapper = wrapper;
		this.profiler = profiler;
	}

	@Override
	public void write(int b) throws IOException {
		profiler.runWrite((s) -> {
			s.set(Byte.SIZE);
			wrapper.write(b);
			return null;
		});
	}

	@Override
	public void write(byte[] b) throws IOException {
		profiler.runWrite((s) -> {
			s.set((long) b.length * Byte.SIZE);
			wrapper.write(b);
			return null;
		});
	}

	@Override
	public void write(byte[] b, int off, int len) throws IOException {
		profiler.runWrite((s) -> {
			s.set((long) len * Byte.SIZE);
			wrapper.write(b, off, len);
			return null;
		});
	}

	@Override
	public void flush() throws IOException {
		profiler.runWrite((s) -> {
			wrapper.flush();
			return null;
		});
	}

	@Override
	public void close() throws IOException {
		profiler.runWrite((s) -> {
			wrapper.close();
			return null;
		});
	}

	public Profiler getProfiler() {
		return profiler;
	}
}
