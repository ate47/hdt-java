package org.rdfhdt.hdt.util.io.compress;

import org.rdfhdt.hdt.enums.TripleComponentOrder;
import org.rdfhdt.hdt.hdt.impl.diskimport.CompressTripleMapper;
import org.rdfhdt.hdt.hdt.impl.diskimport.CompressionResult;
import org.rdfhdt.hdt.hdt.impl.diskimport.TripleCompressionResult;
import org.rdfhdt.hdt.iterator.utils.FileTripleIDIterator;
import org.rdfhdt.hdt.listener.ProgressListener;
import org.rdfhdt.hdt.util.concurrent.TreeWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

public class MapCompressTripleMerger implements TreeWorker.TreeWorkerObject<File> {
	private static final Logger log = LoggerFactory.getLogger(MapCompressTripleMerger.class);
	private final FileTripleIDIterator it;
	private final CompressTripleMapper mapper;
	private final int workers;
	private final ProgressListener listener;
	private final TripleComponentOrder order;

	public MapCompressTripleMerger(FileTripleIDIterator it, CompressTripleMapper mapper, int workers, ProgressListener listener, TripleComponentOrder order) {
		this.it = it;
		this.mapper = mapper;
		this.workers = workers;
		this.listener = listener;
		this.order = order;
	}

	@Override
	public File construct(File a, File b) {
		return null;
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
		return null;
	}

	public TripleCompressionResult mergeToFile(int workers) throws TreeWorker.TreeWorkerException, InterruptedException {
		return null;
	}

	public TripleCompressionResult mergeToPartial() {
		return null;
	}

	public TripleCompressionResult merge(int workers, String mode) throws TreeWorker.TreeWorkerException, InterruptedException {
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
