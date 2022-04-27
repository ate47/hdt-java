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
import org.rdfhdt.hdt.util.ParallelSortableArrayList;
import org.rdfhdt.hdt.util.concurrent.TreeWorker;
import org.rdfhdt.hdt.util.io.CloseSuppressPath;
import org.rdfhdt.hdt.util.io.IOUtil;
import org.rdfhdt.hdt.util.listener.ListenerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * TreeWorkerObject implementation to map and merge tripleID from a compress triple file
 * @author Antoine Willerval
 */
public class MapCompressTripleMerger implements TreeWorker.TreeWorkerObject<CloseSuppressPath> {
	private static final Logger log = LoggerFactory.getLogger(MapCompressTripleMerger.class);
	private final AtomicInteger FID = new AtomicInteger();
	private final CloseSuppressPath baseFileName;
	private final FileTripleIDIterator source;
	private final CompressTripleMapper mapper;
	private final ProgressListener listener;
	private final TripleComponentOrder order;
	private boolean done;
	private long triplesCount = 0;
	private final ParallelSortableArrayList<TripleID> tripleIDS = new ParallelSortableArrayList<>(TripleID[].class);

	public MapCompressTripleMerger(CloseSuppressPath baseFileName, FileTripleIDIterator it, CompressTripleMapper mapper, ProgressListener listener, TripleComponentOrder order) {
		this.baseFileName = baseFileName;
		this.source = it;
		this.mapper = mapper;
		this.listener = listener;
		this.order = order;
	}

	@Override
	public CloseSuppressPath construct(CloseSuppressPath a, CloseSuppressPath b) {
		try {
			int fid = FID.incrementAndGet();
			CloseSuppressPath triplesFiles = baseFileName.resolve( "triples" + fid + ".raw");
			try (
					CompressTripleWriter w = new CompressTripleWriter(triplesFiles.openOutputStream(true));
					CompressTripleReader r1 = new CompressTripleReader(a.openInputStream(true));
					CompressTripleReader r2 = new CompressTripleReader(b.openInputStream(true))) {
				CompressTripleMergeIterator mergeIterator = new CompressTripleMergeIterator(r1, r2, order);
				mergeIterator.forEachRemaining(w::appendTriple);
			}
			return triplesFiles;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void delete(CloseSuppressPath f) {
		try {
			f.close();
		} catch (IOException e) {
			log.warn("Can't delete triple file {}", f, e);
		}
	}

	@Override
	public CloseSuppressPath get() {
		try {
			if (done || !source.hasNewFile()) {
				done = true;
				return null;
			}
			tripleIDS.clear();
			while (source.hasNext()) {
				if (tripleIDS.size() == Integer.MAX_VALUE - 5) {
					source.forceNewFile();
					continue;
				}
				TripleID next = source.next();
				tripleIDS.add(new TripleID(
						mapper.extractSubject(next.getSubject()),
						mapper.extractPredicate(next.getPredicate()),
						mapper.extractObjects(next.getObject())
				));
				triplesCount++;
				ListenerUtil.notifyCond(listener, "Merging triples", triplesCount, triplesCount, 100);
			}

			tripleIDS.parallelSort(TripleIDComparator.getComparator(order));
			int fid = FID.incrementAndGet();
			CloseSuppressPath triplesFiles = baseFileName.resolve("triples" + fid + ".raw");
			try (CompressTripleWriter w = new CompressTripleWriter(triplesFiles.openOutputStream(true))) {
				TripleID prev = new TripleID(-1,-1,-1);
				for (TripleID triple : tripleIDS) {
					if (prev.match(triple)) {
						continue;
					}
					prev.setAll(triple.getSubject(), triple.getPredicate(), triple.getObject());
					w.appendTriple(triple);
				}
			} finally {
				tripleIDS.clear();
			}
			return triplesFiles;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * merge these triples into a file
	 * @param workers number of worker
	 * @return result
	 * @throws TreeWorker.TreeWorkerException TreeWorker error
	 * @throws InterruptedException thread interruption
	 * @throws IOException io error
	 */
	public TripleCompressionResult mergeToFile(int workers) throws TreeWorker.TreeWorkerException, InterruptedException, IOException {
		// force to create the first file
		TreeWorker<CloseSuppressPath> treeWorker = new TreeWorker<>(this, workers);
		treeWorker.start();
		// wait for the workers to merge the sections and create the triples
		CloseSuppressPath triples = treeWorker.waitToComplete();
		return new TripleCompressionResultFile(triplesCount, triples, order);
	}

	/**
	 * merge these triples while reading them, increase the memory usage
	 * @return result
	 * @throws IOException io error
	 */
	public TripleCompressionResult mergeToPartial() throws IOException {
		CloseSuppressPath f;
		List<CloseSuppressPath> files = new ArrayList<>();
		try {
			while ((f = get()) != null) {
				files.add(f);
			}
		} catch (RuntimeException e) {
			IOUtil.closeAll(files);
			throw e;
		}
		return new TripleCompressionResultPartial(files, triplesCount, order);
	}

	/**
	 * merge the triples into a result
	 * @param workers number of workers (complete mode)
	 * @param mode the mode of merging
	 * @return result
	 * @throws TreeWorker.TreeWorkerException TreeWorker error (complete mode)
	 * @throws InterruptedException thread interruption (complete mode)
	 * @throws IOException io error
	 */
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
