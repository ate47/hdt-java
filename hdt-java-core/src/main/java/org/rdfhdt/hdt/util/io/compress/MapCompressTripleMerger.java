package org.rdfhdt.hdt.util.io.compress;

import org.rdfhdt.hdt.enums.TripleComponentOrder;
import org.rdfhdt.hdt.hdt.impl.diskimport.CompressTripleMapper;
import org.rdfhdt.hdt.hdt.impl.diskimport.CompressionResult;
import org.rdfhdt.hdt.hdt.impl.diskimport.TripleCompressionResult;
import org.rdfhdt.hdt.hdt.impl.diskimport.TripleCompressionResultFile;
import org.rdfhdt.hdt.hdt.impl.diskimport.TripleCompressionResultPartial;
import org.rdfhdt.hdt.iterator.utils.FileTripleIDIterator;
import org.rdfhdt.hdt.listener.ProgressListener;
import org.rdfhdt.hdt.triples.TripleID;
import org.rdfhdt.hdt.triples.TripleIDComparator;
import org.rdfhdt.hdt.util.concurrent.TreeWorker;
import org.rdfhdt.hdt.util.listener.ListenerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class MapCompressTripleMerger implements TreeWorker.TreeWorkerObject<File> {
	private static final Logger log = LoggerFactory.getLogger(MapCompressTripleMerger.class);
	private final AtomicInteger FID = new AtomicInteger();
	private final String baseFileName;
	private final FileTripleIDIterator source;
	private final CompressTripleMapper mapper;
	private final ProgressListener listener;
	private final TripleComponentOrder order;
	private boolean done;
	private long triplesCount = 0;

	public MapCompressTripleMerger(String baseFileName, FileTripleIDIterator it, CompressTripleMapper mapper, ProgressListener listener, TripleComponentOrder order) {
		this.baseFileName = baseFileName;
		this.source = it;
		this.mapper = mapper;
		this.listener = listener;
		this.order = order;
	}

	@Override
	public File construct(File a, File b) {
		try {
			int fid = FID.incrementAndGet();
			File triplesFiles = new File(baseFileName, "triples" + fid + ".raw");
			try (
					CompressTripleWriter w = new CompressTripleWriter(new FileOutputStream(triplesFiles));
					CompressTripleReader r1 = new CompressTripleReader(new FileInputStream(a));
					CompressTripleReader r2 = new CompressTripleReader(new FileInputStream(b))) {
				CompressTripleMergeIterator mergeIterator = new CompressTripleMergeIterator(r1, r2, order);
				mergeIterator.forEachRemaining(w::appendTriple);
			}
			return triplesFiles;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void delete(File f) {
		try {
			Files.deleteIfExists(f.toPath());
		} catch (IOException e) {
			log.warn("Can't delete triple file {}", f, e);
		}
	}

	@Override
	public File get() {
		try {
			if (done || !source.hasNewFile()) {
				done = true;
				return null;
			}
			List<TripleID> triples = new ArrayList<>();
			while (source.hasNext()) {
				if (triples.size() == Integer.MAX_VALUE - 5) {
					source.forceNewFile();
					continue;
				}
				TripleID next = source.next();
				triples.add(new TripleID(
						mapper.extractSubject(next.getSubject()),
						mapper.extractPredicate(next.getPredicate()),
						mapper.extractObjects(next.getObject())
				));
				triplesCount++;
				ListenerUtil.notifyCond(listener, "Merging triples", triplesCount, triplesCount, 100);
			}

			triples.sort(TripleIDComparator.getComparator(order));
			int fid = FID.incrementAndGet();
			File triplesFiles = new File(baseFileName, "triples" + fid + ".raw");
			try (CompressTripleWriter w = new CompressTripleWriter(new FileOutputStream(triplesFiles))) {
				TripleID prev = new TripleID(-1,-1,-1);
				for (TripleID triple : triples) {
					if (prev.match(triple)) {
						continue;
					}
					prev.setAll(triple.getSubject(), triple.getPredicate(), triple.getObject());
					w.appendTriple(triple);
				}
			}
			return triplesFiles;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public TripleCompressionResult mergeToFile(int workers) throws TreeWorker.TreeWorkerException, InterruptedException, IOException {
		// force to create the first file
		TreeWorker<File> treeWorker = new TreeWorker<>(this, workers);
		treeWorker.start();
		// wait for the workers to merge the sections and create the triples
		File triples = treeWorker.waitToComplete();
		return new TripleCompressionResultFile(triplesCount, triples, order);
	}

	public TripleCompressionResult mergeToPartial() throws IOException {
		File f;
		List<File> files = new ArrayList<>();
		try {
			while ((f = get()) != null) {
				files.add(f);
			}
		} catch (RuntimeException e) {
			files.forEach(ff -> {
				try {
					Files.deleteIfExists(ff.toPath());
				} catch (IOException ee) {
					log.warn("Can't delete " + ff + " " + ee);
				}
			});
			throw e;
		}
		return new TripleCompressionResultPartial(files, triplesCount, order);
	}

	public TripleCompressionResult merge(int workers, String mode) throws TreeWorker.TreeWorkerException, InterruptedException, IOException {
		if (mode == null) {
			mode = "";
		}
		switch (mode) {
			case "":
			case CompressionResult.COMPRESSION_MODE_COMPLETE:
				return mergeToFile(workers);
			case CompressionResult.COMPRESSION_MODE_PARTIAL:
				return mergeToPartial();
			default:
				throw new IllegalArgumentException("Unknown compression mode: " + mode);
		}
	}
}
