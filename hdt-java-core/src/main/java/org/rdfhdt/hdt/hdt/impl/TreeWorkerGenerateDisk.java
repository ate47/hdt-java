package org.rdfhdt.hdt.hdt.impl;

import org.rdfhdt.hdt.exceptions.ParserException;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.iterator.utils.FileTripleIterator;
import org.rdfhdt.hdt.listener.ProgressListener;
import org.rdfhdt.hdt.options.HDTOptions;
import org.rdfhdt.hdt.triples.TripleString;
import org.rdfhdt.hdt.util.concurrent.TreeWorker;
import org.rdfhdt.hdt.util.io.CompressNodeWriter;
import org.rdfhdt.hdt.util.string.CharSequenceComparator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
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
		long maxMemory = Runtime.getRuntime().maxMemory();
		return ((maxMemory - 1024 * 1024 * 1024)) / processors;
	}

	public static HDT generateHDT(Iterator<TripleString> iterator, HDTOptions hdtFormat, ProgressListener listener) throws IOException, ParserException {
		int workers = Runtime.getRuntime().availableProcessors();
		long chunkSize = getMaxChunkSize(workers);
		FileTripleIterator it = new FileTripleIterator(iterator, chunkSize);
		try (TreeWorkerGenerateDisk obj = new TreeWorkerGenerateDisk(it, hdtFormat)) {
			// force to create the first file
			obj.getFile(0);
			try {
				TreeWorker<File> treeWorkerSubject = new TreeWorker<>(obj::construct, () -> obj.getFile(1), obj::delete, workers);
				TreeWorker<File> treeWorkerObject = new TreeWorker<>(obj::construct, () -> obj.getFile(2), obj::delete, workers);
				TreeWorker<File> treeWorkerPredicate = new TreeWorker<>(obj::construct, () -> obj.getFile(3), obj::delete, workers);
				treeWorkerSubject.start();
				treeWorkerPredicate.start();
				treeWorkerObject.start();

				File sf = treeWorkerSubject.waitToComplete();
				File pf = treeWorkerPredicate.waitToComplete();
				File of = treeWorkerObject.waitToComplete();
			} catch (TreeWorker.TreeWorkerException | InterruptedException e) {
				throw new ParserException(e);
			}
		}
		return null;
	}

	private final String baseFileName = "temp_gen_" + UUID.randomUUID() + "_";
	private final TripleMapper mapper = new TripleMapper() {
	};
	private final FileTripleIterator source;
	private final HDTOptions spec;
	private boolean done;
	private final Object NEXT_SYNC = new Object() {
	};
	private NextFile next;
	private final FileWriter writer;

	TreeWorkerGenerateDisk(FileTripleIterator source, HDTOptions spec) throws IOException {
		this.source = source;
		this.spec = spec;
		this.writer = new FileWriter(baseFileName + "triples.raw");
	}


	public File getFile(int id) {
		try {
			synchronized (NEXT_SYNC) {
				if (next == null || next.isCompleted()) {
					if (done || !source.hasNewFile()) {
						done = true;
						return null;
					}

					List<CharSequence> subjects = new ArrayList<>();
					List<CharSequence> predicates = new ArrayList<>();
					List<CharSequence> objects = new ArrayList<>();

					while (source.hasNext()) {
						TripleString next = source.next();
						CompressNodeWriter.writeCompress(next, writer);
						subjects.add(mapper.convertSubject(next.getSubject()));
						predicates.add(mapper.convertPredicate(next.getPredicate()));
						objects.add(mapper.convertObject(next.getObject()));
					}

					int fid = ID_INC.incrementAndGet();

					subjects.sort(CharSequenceComparator.getInstance());
					File subjectsFile = new File(baseFileName + "subjects" + fid + ".raw");
					CompressNodeWriter.writeCompress(subjects, subjectsFile, spec);

					predicates.sort(CharSequenceComparator.getInstance());
					File predicatesFile = new File(baseFileName + "predicates" + fid + ".raw");
					CompressNodeWriter.writeCompress(predicates, predicatesFile, spec);

					objects.sort(CharSequenceComparator.getInstance());
					File objectsFile = new File(baseFileName + "objects" + fid + ".raw");
					CompressNodeWriter.writeCompress(objects, objectsFile, spec);
				}
				switch (id) {
					case 1: // s
						if (next.isSubjectGet()) {
							NEXT_SYNC.wait();
						}
						return next == null ? null : next.getAsSubject();
					case 2: // p
						if (next.isPredicateGet()) {
							NEXT_SYNC.wait();
						}
						return next == null ? null : next.getAsPredicate();
					case 3: // o
						if (next.isObjectGet()) {
							NEXT_SYNC.wait();
						}
						return next == null ? null : next.getAsObject();
				}
			}

		} catch (InterruptedException | IOException e) {
			throw new RuntimeException(e);
		}
		return null;
	}

	public File construct(File a, File b) {
		return null;
	}

	public void delete(File f) {
		try {
			Files.deleteIfExists(f.toPath());
		} catch (IOException e) {
			log.warn("Can't delete temp file {}", f);
		}
	}

	@Override
	public void close() throws IOException {
		writer.close();
	}

	public interface TripleMapper {
		default CharSequence convertSubject(CharSequence seq) {
			return seq;
		}
		default CharSequence convertPredicate(CharSequence seq) {
			return seq;
		}
		default CharSequence convertObject(CharSequence seq) {
			return seq;
		}
	}

	private static class NextFile {
		private boolean subjectGet, predicateGet, objectGet;
		private final File subjectFile, predicateFile, objectFile;

		public NextFile(File subjectFile, File predicateFile, File objectFile) {
			this.subjectFile = subjectFile;
			this.predicateFile = predicateFile;
			this.objectFile = objectFile;
		}

		public boolean isSubjectGet() {
			return subjectGet;
		}

		public boolean isPredicateGet() {
			return predicateGet;
		}

		public boolean isObjectGet() {
			return objectGet;
		}

		public boolean isCompleted() {
			return isSubjectGet() && isPredicateGet() && isObjectGet();
		}

		public File getAsSubject() {
			subjectGet = true;
			return subjectFile;
		}
		public File getAsPredicate() {
			predicateGet = true;
			return predicateFile;
		}
		public File getAsObject() {
			objectGet = true;
			return objectFile;
		}
	}


}
