package org.rdfhdt.hdt.hdt.impl.diskimport;

import org.rdfhdt.hdt.iterator.utils.FileTripleIterator;
import org.rdfhdt.hdt.listener.ProgressListener;
import org.rdfhdt.hdt.triples.IndexedNode;
import org.rdfhdt.hdt.triples.IndexedTriple;
import org.rdfhdt.hdt.triples.TripleString;
import org.rdfhdt.hdt.util.concurrent.TreeWorker;
import org.rdfhdt.hdt.util.io.compress.CompressTripleWriter;
import org.rdfhdt.hdt.util.io.compress.CompressUtil;
import org.rdfhdt.hdt.util.listener.ListenerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class SectionCompressor implements Closeable, TreeWorker.TreeWorkerCat<SectionCompressor.TripleFile>, TreeWorker.TreeWorkerDelete<SectionCompressor.TripleFile>, TreeWorker.TreeWorkerSupplier<SectionCompressor.TripleFile> {
	private static final AtomicInteger ID_INC = new AtomicInteger();
	private static final Logger log = LoggerFactory.getLogger(SectionCompressor.class);

	private final String baseFileName;
	private final FileTripleIterator source;
	private final CompressTripleWriter writer;
	private boolean done;
	private final File triplesOutput;
	private final ProgressListener listener;
	private long triples = 0;
	private final IdFetcher subjectIdFetcher = new IdFetcher();
	private final IdFetcher predicateIdFetcher = new IdFetcher();
	private final IdFetcher objectIdFetcher = new IdFetcher();

	public SectionCompressor(String baseFileName, FileTripleIterator source, ProgressListener listener) throws IOException {
		this.source = source;
		this.listener = listener;
		this.baseFileName = baseFileName;
		this.triplesOutput = new File(baseFileName + "triples.raw");
		this.writer = new CompressTripleWriter(new FileOutputStream(triplesOutput));
	}

	/**
	 * @return the next file to merge
	 */
	@Override
	public SectionCompressor.TripleFile get() {
		try {
			if (done || !source.hasNewFile()) {
				done = true;
				return null;
			}

			List<IndexedNode> subjects = new ArrayList<>();
			List<IndexedNode> predicates = new ArrayList<>();
			List<IndexedNode> objects = new ArrayList<>();
			IndexedNode lastS = IndexedNode.UNKNOWN, lastP = IndexedNode.UNKNOWN, lastO = IndexedNode.UNKNOWN;
			IndexedTriple triple = new IndexedTriple();
			while (source.hasNext()) {
				// too much ram allowed?
				if (subjects.size() == Integer.MAX_VALUE - 5) {
					source.forceNewFile();
					continue;
				}
				TripleString next = source.next();
				// get indexed mapped char sequence
				CharSequence sc = convertSubject(next.getSubject());
				long s = subjectIdFetcher.getNodeId(sc);
				if (s != lastS.getIndex()) {
					// create new node if not the same as the previous one
					subjects.add(lastS = new IndexedNode(sc, s));
				}

				// get indexed mapped char sequence
				CharSequence pc = convertPredicate(next.getPredicate());
				long p = predicateIdFetcher.getNodeId(pc);
				if (p != lastP.getIndex()) {
					// create new node if not the same as the previous one
					predicates.add(lastP = new IndexedNode(pc, p));
				}

				// get indexed mapped char sequence
				CharSequence oc = convertObject(next.getObject());
				long o = objectIdFetcher.getNodeId(oc);
				if (o != lastO.getIndex()) {
					// create new node if not the same as the previous one
					objects.add(lastO = new IndexedNode(oc, o));
				}

				// load the map triple and write it in the writer
				triple.load(lastS, lastP, lastO);
				triples++;
				writer.appendTriple(triple);

				ListenerUtil.notifyCond(listener, "Reading triples", triples, triples, 100);
			}
			ListenerUtil.notify(listener, "Writing sections", triples, 100);

			int fid = ID_INC.incrementAndGet();
			TripleFile sections = new TripleFile(new File(baseFileName + "section" + fid + ".raw"));
			try (OutputStream stream = sections.openWSubject()) {
				subjects.sort(IndexedNode::compareTo);
				CompressUtil.writeCompressedSection(subjects, stream);
			}
			try (OutputStream stream = sections.openWPredicate()) {
				predicates.sort(IndexedNode::compareTo);
				CompressUtil.writeCompressedSection(predicates, stream);
			}
			try (OutputStream stream = sections.openWObject()) {
				objects.sort(IndexedNode::compareTo);
				CompressUtil.writeCompressedSection(objects, stream);
			}
			return sections;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}


	/**
	 * merge two section files into a 3rd one
	 *
	 * @param a sections file 1
	 * @param b sections file 2
	 * @return the 3rd section file
	 */
	@Override
	public SectionCompressor.TripleFile construct(SectionCompressor.TripleFile a, SectionCompressor.TripleFile b) {
		int fid = ID_INC.incrementAndGet();
		TripleFile sections;
		try {
			sections = new TripleFile(baseFileName + "section" + fid + ".raw");

			// subjects
			try (OutputStream output = sections.openWSubject();
				 InputStream input1 = a.openRSubject();
				 InputStream input2 = b.openRSubject()) {
				// merge the two nodes together
				CompressUtil.mergeCompressedSection(input1, input2, output);
			}
			// predicates
			try (OutputStream output = sections.openWPredicate();
				 InputStream input1 = a.openRPredicate();
				 InputStream input2 = b.openRPredicate()) {
				// merge the two nodes together
				CompressUtil.mergeCompressedSection(input1, input2, output);
			}
			// objects
			try (OutputStream output = sections.openWObject();
				 InputStream input1 = a.openRObject();
				 InputStream input2 = b.openRObject()) {
				// merge the two nodes together
				CompressUtil.mergeCompressedSection(input1, input2, output);
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		// delete old sections
		delete(a);
		delete(b);
		return sections;
	}

	/**
	 * delete the file f if it exists or warn
	 *
	 * @param f the file to delete
	 */
	@Override
	public void delete(SectionCompressor.TripleFile f) {
		f.delete();
	}

	@Override
	public void close() throws IOException {
		writer.close();
	}

	/*
	 * TODO: create a factory and override these methods with the hdt spec
	 */

	/**
	 * mapping method for the subject of the triple, this method should copy the sequence!
	 *
	 * @param seq the subject (before)
	 * @return the subject mapped
	 */
	protected CharSequence convertSubject(CharSequence seq) {
		return seq.toString();
	}

	/**
	 * mapping method for the predicate of the triple, this method should copy the sequence!
	 *
	 * @param seq the predicate (before)
	 * @return the predicate mapped
	 */
	protected CharSequence convertPredicate(CharSequence seq) {
		return seq.toString();
	}

	/**
	 * mapping method for the object of the triple, this method should copy the sequence!
	 *
	 * @param seq the object (before)
	 * @return the object mapped
	 */
	protected CharSequence convertObject(CharSequence seq) {
		return seq.toString();
	}

	/**
	 * Compress the stream into complete pre-sections files
	 *
	 * @param workers the number of workers
	 * @return compression result
	 * @throws IOException                    io exception
	 * @throws InterruptedException           if the thread is interrupted
	 * @throws TreeWorker.TreeWorkerException exception with the tree working
	 * @see #compressPartial()
	 * @see #compress(int, String)
	 */
	public CompressionResult compressToFile(int workers) throws IOException, InterruptedException, TreeWorker.TreeWorkerException {
		// force to create the first file
		TreeWorker<TripleFile> treeWorker = new TreeWorker<>(this, workers);
		treeWorker.start();
		// wait for the workers to merge the sections and create the triples
		TripleFile sections = treeWorker.waitToComplete();
		return new CompressionResultFile(triplesOutput, triples, sections);
	}

	/**
	 * Compress the stream into multiple pre-sections files and merge them on the fly
	 *
	 * @return compression result
	 * @throws IOException io exception
	 * @see #compressToFile(int)
	 * @see #compress(int, String)
	 */
	public CompressionResult compressPartial() throws IOException {
		TripleFile f;
		List<TripleFile> files = new ArrayList<>();
		try {
			while ((f = get()) != null) {
				files.add(f);
			}
		} catch (RuntimeException e) {
			files.forEach(TripleFile::delete);
			throw e;
		}
		return new CompressionResultPartial(files, triplesOutput, triples);
	}

	/**
	 * compress the sections/triples with a particular mode
	 *
	 * @param workers the worker required
	 * @param mode    the mode to compress, can be {@link org.rdfhdt.hdt.hdt.impl.diskimport.CompressionResult#COMPRESSION_MODE_COMPLETE} (default), {@link org.rdfhdt.hdt.hdt.impl.diskimport.CompressionResult#COMPRESSION_MODE_PARTIAL} or null/"" for default
	 * @return the compression result
	 * @throws TreeWorker.TreeWorkerException tree working exception
	 * @throws IOException io exception
	 * @throws InterruptedException thread interruption
	 * @see #compressToFile(int)
	 * @see #compressPartial()
	 */
	public CompressionResult compress(int workers, String mode) throws TreeWorker.TreeWorkerException, IOException, InterruptedException {
		if (mode == null) {
			mode = "";
		}
		switch (mode) {
			case "":
			case CompressionResult.COMPRESSION_MODE_COMPLETE:
				return compressToFile(workers);
			case CompressionResult.COMPRESSION_MODE_PARTIAL:
				return compressPartial();
			default:
				throw new IllegalArgumentException("Unknown compression mode: " + mode);
		}
	}

	public static class TripleFile {
		public final File root;
		public final File s;
		public final File p;
		public final File o;

		public TripleFile(String root) throws IOException {
			this(new File(root));
		}

		public TripleFile(File root) throws IOException {
			this.root = root;
			this.s = new File(root, "subject");
			this.p = new File(root, "predicate");
			this.o = new File(root, "object");

			Files.createDirectories(root.toPath());
		}

		public void delete() {
			try {
				Files.deleteIfExists(s.toPath());
				Files.deleteIfExists(p.toPath());
				Files.deleteIfExists(o.toPath());
				Files.delete(root.toPath());
			} catch (IOException e) {
				log.warn("Can't delete sections {}: {}", root, e);
			}
		}

		public OutputStream openWSubject() throws IOException {
			return new FileOutputStream(s);
		}

		public OutputStream openWPredicate() throws IOException {
			return new FileOutputStream(p);
		}

		public OutputStream openWObject() throws IOException {
			return new FileOutputStream(o);
		}

		public InputStream openRSubject() throws IOException {
			return new FileInputStream(s);
		}

		public InputStream openRPredicate() throws IOException {
			return new FileInputStream(p);
		}

		public InputStream openRObject() throws IOException {
			return new FileInputStream(o);
		}
	}

	private static class IdFetcher {
		private CharSequence previous;
		private long id = 1;

		public long getNodeId(CharSequence sec) {
			if (sec.equals(previous)) {
				return id;
			}
			previous = sec;
			return ++id;
		}

		public long getCount() {
			return id;
		}
	}
}
