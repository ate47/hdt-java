package org.rdfhdt.hdt.util;

import org.rdfhdt.hdt.options.HDTOptions;
import org.rdfhdt.hdt.options.HDTOptionsKeys;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * tool to profile time and io
 *
 * @author Antoine Willerval
 */
public class Profiler implements AutoCloseable {
	private static Profiler global;
	private static final AtomicLong PROFILER_IDS = new AtomicLong();
	private static final Map<Long, Profiler> PROFILER = new HashMap<>();

	/**
	 * @return global profiler (if defined)
	 */
	public static Profiler getGlobal() {
		return global;
	}

	/**
	 * get a non-closed profiler
	 *
	 * @param id profiler id
	 * @return profiler or null if closed or non-existing
	 */
	public static Profiler getProfilerById(long id) {
		return PROFILER.get(id);
	}

	/**
	 * Read the profiling values from an input path
	 *
	 * @param inputPath input path
	 * @throws java.io.IOException                reading exception
	 * @throws java.lang.IllegalArgumentException if the file's CRC doesn't match
	 */
	public static Profiler readFromDisk(Path inputPath) throws IOException {
		Profiler p = new Profiler("");
		try (InputStream is = new BufferedInputStream(Files.newInputStream(inputPath))) {
			for (byte b : HEADER) {
				if (is.read() != b) {
					throw new IOException("Missing header for the profiling file!");
				}
			}
			p.mainSection = p.new Section(is, 0);
			int checkSum = p.mainSection.computeCheckSum();
			int checkSumRead = (int) readLong(is);
			if (checkSumRead != checkSum) {
				throw new IOException("the Checksum isn't the same");
			}
		}
		return p;
	}

	private static long readLong(InputStream is) throws IOException {
		byte[] longBuffer = readBuffer(is, 8);
		return (longBuffer[0] & 0xFF)
				| ((longBuffer[1] & 0xFFL) << 8)
				| ((longBuffer[2] & 0xFFL) << 16)
				| ((longBuffer[3] & 0xFFL) << 24)
				| ((longBuffer[4] & 0xFFL) << 32)
				| ((longBuffer[5] & 0xFFL) << 40)
				| ((longBuffer[6] & 0xFFL) << 48)
				| ((longBuffer[7] & 0xFFL) << 56);
	}

	private static void writeLong(OutputStream os, long value) throws IOException {
		os.write((byte) (value & 0xFF));
		os.write((byte) ((value >>> 8) & 0xFF));
		os.write((byte) ((value >>> 16) & 0xFF));
		os.write((byte) ((value >>> 24) & 0xFF));
		os.write((byte) ((value >>> 32) & 0xFF));
		os.write((byte) ((value >>> 40) & 0xFF));
		os.write((byte) ((value >>> 48) & 0xFF));
		os.write((byte) ((value >>> 56) & 0xFF));
	}

	private static byte[] readBuffer(InputStream input, int length) throws IOException {
		int nRead;
		int pos = 0;
		byte[] data = new byte[length];

		while ((nRead = input.read(data, pos, length - pos)) > 0) {
			pos += nRead;
		}

		if (pos != length) {
			throw new EOFException("EOF while reading array from InputStream");
		}

		return data;
	}

	/**
	 * create or load a profiler from the options into a subsection
	 *
	 * @param name    name
	 * @param options options
	 * @param setId   set the id after loading (if required)
	 * @return profiler
	 */
	public static Profiler createOrLoadSubSection(String name, HDTOptions options, boolean setId) {
		// no options, we can't create
		if (options == null) {
			return new Profiler(name, null);
		}
		String profiler = options.get(HDTOptionsKeys.PROFILER_KEY);
		if (profiler != null && profiler.length() != 0 && profiler.charAt(0) == '!') {
			Profiler prof = getProfilerById(Long.parseLong(profiler.substring(1)));
			if (prof != null) {
				prof.pushSection(name);
				prof.deep++;
				return prof;
			}
		}
		// no id, not an id
		Profiler prof = new Profiler(name, options);
		if (setId) {
			options.set(HDTOptionsKeys.PROFILER_KEY, prof);
		}
		return prof;
	}

	private static final byte[] HEADER = {'H', 'D', 'T', 'P', 'R', 'O', 'F', 'I', 'L', 'E'};
	private int maxSize = 0;
	private final String name;
	private Section mainSection;
	private boolean disabled;
	private boolean io;
	private Path outputPath;
	private final long id;
	private int deep = 0;

	/**
	 * create a disabled profiler
	 *
	 * @param name the profiler name
	 */
	public Profiler(String name) {
		this(name, null);
	}

	/**
	 * create a profiler from specifications
	 *
	 * @param name profiler name
	 * @param spec spec (nullable)
	 */
	public Profiler(String name, HDTOptions spec) {
		this.id = PROFILER_IDS.incrementAndGet();
		PROFILER.put(this.id, this);
		this.name = Objects.requireNonNull(name, "name can't be null!");
		if (spec != null) {
			String b = spec.get(HDTOptionsKeys.PROFILER_KEY);
			disabled = b == null || b.length() == 0 || !(b.charAt(0) == '!' || "true".equalsIgnoreCase(b));
			String profilerOutputLocation = spec.get(HDTOptionsKeys.PROFILER_OUTPUT_KEY);
			if (profilerOutputLocation != null && !profilerOutputLocation.isEmpty()) {
				outputPath = Path.of(profilerOutputLocation);
			}
			io = spec.getBoolean(HDTOptionsKeys.PROFILER_IO_KEY, false);
		} else {
			// no profiling by default
			disabled = true;
		}
	}

	/**
	 * disable the profiler methods
	 *
	 * @param disable if true, the methods will be callable, but won't do anything
	 */
	public void setDisabled(boolean disable) {
		this.disabled = disable;
	}

	/**
	 * start a section
	 *
	 * @param name the section name
	 */
	public void pushSection(String name) {
		if (disabled) {
			return;
		}
		getMainSection().pushSection(name, 0);
	}

	/**
	 * @return profiler id
	 */
	public long getId() {
		return id;
	}

	public boolean isDisabled() {
		return disabled;
	}

	/**
	 * @return is the io profiling enabled, useless if {@link #setGlobal()} isn't called
	 */
	public boolean isIoProfiling() {
		return io;
	}

	/**
	 * set this profiler as global to handle IO profiling
	 */
	public void setGlobal() {
		if (global != null && global != this) {
			throw new IllegalArgumentException("Can't set this profiler as global because another one is already in use");
		}
		global = this;
	}

	/**
	 * unset (if was set) this profiler as global
	 */
	public void unsetGlobal() {
		if (global == this) {
			global = null;
		}
	}

	/**
	 * complete a section
	 */
	public void popSection() {
		if (disabled) {
			return;
		}
		if (!getMainSection().isRunning()) {
			throw new IllegalArgumentException("profiler not running!");
		}
		getMainSection().popSection();
	}

	/**
	 * stop the profiler without poping sections
	 */
	public void stop() {
		if (disabled || deep != 0) {
			return;
		}
		getMainSection().stop();
	}

	/**
	 * reset the profiler
	 */
	public void reset() {
		mainSection = null;
	}

	/**
	 * write the profile into the console
	 */
	public void writeProfiling() throws IOException {
		if (disabled || deep != 0) {
			return;
		}
		getMainSection().writeProfiling("", true);
		if (outputPath != null) {
			writeToDisk(outputPath);
		}
	}

	/**
	 * Write the profiling values into the output path
	 *
	 * @param outputPath output path
	 */
	public void writeToDisk(Path outputPath) throws IOException {
		try (OutputStream os = new BufferedOutputStream(Files.newOutputStream(outputPath))) {
			for (byte b : HEADER) {
				os.write(b);
			}
			Section mainSection = getMainSection();
			mainSection.writeSection(os);
			writeLong(os, mainSection.computeCheckSum());
		}
	}

	/**
	 * @return the main section of the profiler
	 */
	public Section getMainSection() {
		if (this.mainSection == null) {
			this.mainSection = new Section(name);
		}
		return this.mainSection;
	}

	@Override
	public void close() {
		unsetGlobal();
		if (deep == 0) {
			PROFILER.remove(getId());
		} else {
			deep--;
			popSection();
		}
	}

	/**
	 * a section in the profiling
	 */
	public class Section {
		private final String name;
		private final long start;
		private long end;
		private long totalIORead;
		private long lastIORead;
		private long totalIOWrite;
		private long lastIOWrite;
		private long bytesIORead;
		private long bytesIOWrite;
		private final List<Section> subSections;
		private transient Section currentSection;

		Section(String name) {
			this.name = name;
			start = System.currentTimeMillis();
			end = start;
			subSections = new ArrayList<>();
		}

		/**
		 * read the section from the input stream
		 *
		 * @param is input stream
		 * @throws IOException io exception
		 */
		Section(InputStream is, int deep) throws IOException {
			start = readLong(is);
			end = readLong(is);

			totalIORead = readLong(is);
			lastIORead = readLong(is);
			totalIOWrite = readLong(is);
			lastIOWrite = readLong(is);
			bytesIORead = readLong(is);
			bytesIOWrite = readLong(is);

			int nameLength = (int) readLong(is);
			byte[] nameBytes = readBuffer(is, nameLength);
			name = new String(nameBytes, StandardCharsets.UTF_8);

			maxSize = Math.max(name.length() + deep * 2, maxSize);

			int subSize = (int) readLong(is);
			subSections = new ArrayList<>(subSize);
			for (int i = 0; i < subSize; i++) {
				subSections.add(new Section(is, deep + 1));
			}
		}

		void writeSection(OutputStream os) throws IOException {
			byte[] nameBytes = name.getBytes(StandardCharsets.UTF_8);

			writeLong(os, start);
			writeLong(os, end);

			writeLong(os, totalIORead);
			writeLong(os, lastIORead);
			writeLong(os, totalIOWrite);
			writeLong(os, lastIOWrite);
			writeLong(os, bytesIORead);
			writeLong(os, bytesIOWrite);

			writeLong(os, nameBytes.length);
			os.write(nameBytes);

			List<Section> sub = getSubSections();
			writeLong(os, sub.size());

			for (Section s : sub) {
				s.writeSection(os);
			}
		}

		/**
		 * @return the subsections
		 */
		public List<Section> getSubSections() {
			return subSections;
		}

		/**
		 * @return the section name
		 */
		public String getName() {
			return name;
		}

		boolean isRunning() {
			return currentSection != null;
		}

		void pushSection(String name, int deep) {
			if (isRunning()) {
				currentSection.pushSection(name, deep + 1);
				return;
			}

			subSections.add(currentSection = new Section(name));
			maxSize = Math.max(name.length() + deep * 2, maxSize);
		}

		boolean popSection() {
			if (isRunning()) {
				if (currentSection.popSection()) {
					currentSection = null;
				}
				return false;
			} else {
				end = System.currentTimeMillis();
				return true;
			}
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}

			Section section = (Section) o;

			return start == section.start
					&& end == section.end
					&& name.equals(section.name)
					&& totalIORead == section.totalIORead
					&& lastIORead == section.lastIORead
					&& totalIOWrite == section.totalIOWrite
					&& lastIOWrite == section.lastIOWrite
					&& bytesIORead == section.bytesIORead
					&& bytesIOWrite == section.bytesIOWrite
					&& subSections.equals(section.subSections);
		}

		@Override
		public int hashCode() {
			int result = name.hashCode();
			result = 31 * result + (int) (start ^ (start >>> 32));
			result = 31 * result + (int) (end ^ (end >>> 32));
			result = 31 * result + (int) (totalIORead ^ (totalIORead >>> 32));
			result = 31 * result + (int) (lastIORead ^ (lastIORead >>> 32));
			result = 31 * result + (int) (totalIOWrite ^ (totalIOWrite >>> 32));
			result = 31 * result + (int) (lastIOWrite ^ (lastIOWrite >>> 32));
			result = 31 * result + (int) (bytesIORead ^ (bytesIORead >>> 32));
			result = 31 * result + (int) (bytesIOWrite ^ (bytesIOWrite >>> 32));
			result = 31 * result + subSections.hashCode();
			return result;
		}

		@Override
		public String toString() {
			return "Section{" +
					"name='" + name + '\'' +
					", start=" + start +
					", end=" + end +
					", subSections=" + subSections +
					", currentSection=" + currentSection +
					'}';
		}

		void stop() {
			if (isRunning()) {
				currentSection.stop();
			}
			end = System.currentTimeMillis();
		}

		public long getMillis() {
			return end - start;
		}

		public void startIOWrite() {
			if (currentSection == null) {
				lastIOWrite = System.nanoTime();
			} else {
				currentSection.startIOWrite();
			}
		}

		public void endIOWrite(long write) {
			if (currentSection == null) {
				if (lastIOWrite != 0) {
					totalIOWrite = System.nanoTime() - lastIOWrite;
					lastIOWrite = 0;
					bytesIOWrite += write;
				} else {
					throw new RuntimeException("IO not started");
				}
			} else {
				currentSection.endIOWrite(write);
			}
		}

		public long getTotalIORead() {
			return totalIORead;
		}

		public long getTotalIOWrite() {
			return totalIOWrite;
		}

		public long getBytesIOReadNano() {
			return totalIORead;
		}

		public long getBytesIOWriteNano() {
			return bytesIOWrite;
		}

		public void startIORead() {
			if (currentSection == null) {
				lastIORead = System.nanoTime();
			} else {
				currentSection.startIORead();
			}
		}

		public void endIORead(long read) {
			if (currentSection == null) {
				if (lastIORead != 0) {
					totalIORead = System.nanoTime() - lastIORead;
					lastIORead = 0;
					bytesIORead += read;
				} else {
					throw new RuntimeException("IO not started");
				}
			} else {
				currentSection.endIORead(read);
			}
		}

		/**
		 * @return start timestamp
		 */
		public long getStartMillis() {
			return start;
		}

		/**
		 * @return end timestamp
		 */
		public long getEndMillis() {
			return end;
		}

		void writeProfiling(String prefix, boolean isLast) {
			System.out.println(prefix + (getSubSections().isEmpty() ? "+--" : "+-+") + " [" + getName() + "] " + "-".repeat(1 + maxSize - getName().length()) + " elapsed=" + getMillis() + "ms" + (isIoProfiling() ? (", w=" + (getTotalIORead() / 1_000_000) + "ms, r=" + (getTotalIOWrite() / 1_000_000) + "ms") : ""));
			for (int i = 0; i < subSections.size(); i++) {
				Section s = subSections.get(i);
				s.writeProfiling(prefix + (isLast ? "  " : "| "), i == subSections.size() - 1);
			}
		}

		/**
		 * @return checksum for the profiling section
		 */
		public int computeCheckSum() {
			int result = name.length();
			result = 31 * result + (int) (start ^ (start >>> 32));
			result = 31 * result + (int) (end ^ (end >>> 32));
			result = 31 * result + (int) (totalIORead ^ (totalIORead >>> 32));
			result = 31 * result + (int) (lastIORead ^ (lastIORead >>> 32));
			result = 31 * result + (int) (totalIOWrite ^ (totalIOWrite >>> 32));
			result = 31 * result + (int) (lastIOWrite ^ (lastIOWrite >>> 32));
			result = 31 * result + (int) (bytesIORead ^ (bytesIORead >>> 32));
			result = 31 * result + (int) (bytesIOWrite ^ (bytesIOWrite >>> 32));
			for (Section subSection : subSections) {
				result = 31 * result ^ subSection.computeCheckSum();
			}
			return result;
		}
	}
}