package org.rdfhdt.hdt.hdt.impl;

import org.rdfhdt.hdt.compact.bitmap.Bitmap64Disk;
import org.rdfhdt.hdt.exceptions.ParserException;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.iterator.utils.FileTripleIterator;
import org.rdfhdt.hdt.listener.ProgressListener;
import org.rdfhdt.hdt.options.HDTOptions;
import org.rdfhdt.hdt.triples.IndexedNode;
import org.rdfhdt.hdt.triples.IndexedTriple;
import org.rdfhdt.hdt.triples.TripleString;
import org.rdfhdt.hdt.util.concurrent.TreeWorker;
import org.rdfhdt.hdt.util.crc.CRC32;
import org.rdfhdt.hdt.util.crc.CRCOutputStream;
import org.rdfhdt.hdt.util.disk.LongArrayDisk;
import org.rdfhdt.hdt.util.io.CompressNodeWriter;
import org.rdfhdt.hdt.util.io.CompressTripleReader;
import org.rdfhdt.hdt.util.listener.ListenerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

public class TreeWorkerGenerateDisk implements Closeable {
	private static final AtomicInteger ID_INC = new AtomicInteger();
	private static final Logger log = LoggerFactory.getLogger(TreeWorkerGenerateDisk.class);
	/**
	 * @return a theoretical maximum amount of memory the JVM will attempt to use
	 */
	static long getMaxChunkSize(int processors) {
		Runtime runtime = Runtime.getRuntime();
		long presFreeMemory = (long) ((runtime.maxMemory() - (runtime.totalMemory() - runtime.freeMemory())) * 0.125 * 0.85);
		return presFreeMemory  / processors;
	}

	public static HDT generateHDT(Iterator<TripleString> iterator, HDTOptions hdtFormat, ProgressListener listener) throws IOException, ParserException {
		int workers = Runtime.getRuntime().availableProcessors();
		long chunkSize = getMaxChunkSize(workers);
		FileTripleIterator it = new FileTripleIterator(iterator, chunkSize);
		File sections, triples;
		String mapFile;
		long tripleCount;
		try (TreeWorkerGenerateDisk obj = new TreeWorkerGenerateDisk(it, hdtFormat, listener)) {
			// force to create the first file
			ListenerUtil.notify(listener, "Sorting sections", 0, 100);
			TreeWorker<File> treeWorker = new TreeWorker<>(obj::construct, obj::getFile, obj::delete, workers);
			treeWorker.start();
			// wait for the workers to merge the sections and create the triples
			sections = treeWorker.waitToComplete();
			triples = obj.getTriplesOutput();
			tripleCount = obj.triples;
			mapFile = obj.getBaseFileName() + "idmap";
			
			ListenerUtil.notify(listener, "Generate HDT", 50, 100);
		} catch (TreeWorker.TreeWorkerException | InterruptedException e) {
			throw new ParserException(e);
		}
		// create sections and triple mapping

		// create mapped triples file

		// sort mapped triple file

		// create bit triples

		// header

		// return the HDT
		return null;
	}

	private final String baseFileName = "temp_gen_" + UUID.randomUUID() + "_";
	private final FileTripleIterator source;
	private final HDTOptions spec;
	private boolean done;
	private final File triplesOutput;
	private final CRCOutputStream writer;
	private final ProgressListener listener;
	private long triples = 0;
	private final IdFetcher subjectIdFetcher = new IdFetcher();
	private final IdFetcher predicateIdFetcher = new IdFetcher();
	private final IdFetcher objectIdFetcher = new IdFetcher();

	protected TreeWorkerGenerateDisk(FileTripleIterator source, HDTOptions spec, ProgressListener listener) throws IOException {
		this.source = source;
		this.spec = spec;
		this.listener = listener;
		this.triplesOutput = new File(baseFileName + "triples.raw");
		this.writer = new CRCOutputStream(new FileOutputStream(triplesOutput), new CRC32());
	}

	/**
	 * @return the next file to merge
	 */
	public File getFile() {
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
				CompressNodeWriter.writeCompress(triple, writer);

				ListenerUtil.notifyCond(listener, "Reading triples", triples, triples,100);
			}
			ListenerUtil.notify(listener, "Writing sections", triples, 100);

			int fid = ID_INC.incrementAndGet();
			File sections = new File(baseFileName + "section" + fid + ".raw");
			try (FileOutputStream stream = new FileOutputStream(sections)) {
				subjects.sort(IndexedNode::compareTo);
				CompressNodeWriter.writeCompress(subjects, stream, spec);

				predicates.sort(IndexedNode::compareTo);
				CompressNodeWriter.writeCompress(predicates, stream, spec);

				objects.sort(IndexedNode::compareTo);
				CompressNodeWriter.writeCompress(objects, stream, spec);
			}
			return sections;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * @return the triple ouput file
	 */
	public File getTriplesOutput() {
		return triplesOutput;
	}

	/**
	 * merge two section files into a 3rd one
	 * @param a sections file 1
	 * @param b sections file 2
	 * @return the 3rd section file
	 */
	public File construct(File a, File b) {
		int fid = ID_INC.incrementAndGet();
		File sections = new File(baseFileName + "section" + fid + ".raw");
		try (FileOutputStream output = new FileOutputStream(sections);
				FileInputStream input1 = new FileInputStream(a);
				FileInputStream input2 = new FileInputStream(b)) {
			// merge the two nodes together
			CompressNodeWriter.mergeCompress(input1, input2, output);
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
	 * @param f the file to delete
	 */
	public void delete(File f) {
		try {
			Files.deleteIfExists(f.toPath());
		} catch (IOException e) {
			log.warn("Can't delete temp file {}", f);
		}
	}

	@Override
	public void close() throws IOException {
		CompressNodeWriter.writeEndTriple(writer);
		writer.writeCRC();
		writer.close();
	}

	/*
	 * TODO: create a factory and override these methods with the hdt spec
	 */
	/**
	 * mapping method for the subject of the triple
	 * @param seq the subject (before)
	 * @return the subject mapped
	 */
	protected CharSequence convertSubject(CharSequence seq) {
		return seq;
	}
	/**
	 * mapping method for the predicate of the triple
	 * @param seq the predicate (before)
	 * @return the predicate mapped
	 */
	protected CharSequence convertPredicate(CharSequence seq) {
		return seq;
	}
	/**
	 * mapping method for the object of the triple
	 * @param seq the object (before)
	 * @return the object mapped
	 */
	protected CharSequence convertObject(CharSequence seq) {
		return seq;
	}

	public String getBaseFileName() {
		return baseFileName;
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
