package org.rdfhdt.hdt.hdt.impl;

import org.rdfhdt.hdt.dictionary.DictionaryFactory;
import org.rdfhdt.hdt.dictionary.DictionaryPrivate;
import org.rdfhdt.hdt.dictionary.impl.CompressFourSectionDictionary;
import org.rdfhdt.hdt.enums.TripleComponentOrder;
import org.rdfhdt.hdt.exceptions.ParserException;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTVocabulary;
import org.rdfhdt.hdt.hdt.impl.diskimport.CompressTripleMapper;
import org.rdfhdt.hdt.hdt.impl.diskimport.CompressionResult;
import org.rdfhdt.hdt.hdt.impl.diskimport.MapOnCallHDT;
import org.rdfhdt.hdt.hdt.impl.diskimport.TripleCompressionResult;
import org.rdfhdt.hdt.header.HeaderPrivate;
import org.rdfhdt.hdt.iterator.utils.AsyncIteratorFetcher;
import org.rdfhdt.hdt.listener.MultiThreadListener;
import org.rdfhdt.hdt.listener.ProgressListener;
import org.rdfhdt.hdt.options.HDTOptions;
import org.rdfhdt.hdt.options.HDTOptionsKeys;
import org.rdfhdt.hdt.triples.TempTriples;
import org.rdfhdt.hdt.triples.TripleString;
import org.rdfhdt.hdt.triples.TriplesPrivate;
import org.rdfhdt.hdt.util.BitUtil;
import org.rdfhdt.hdt.util.Profiler;
import org.rdfhdt.hdt.util.StringUtil;
import org.rdfhdt.hdt.util.concurrent.KWayMerger;
import org.rdfhdt.hdt.util.io.CloseSuppressPath;
import org.rdfhdt.hdt.util.io.compress.MapCompressTripleMerger;
import org.rdfhdt.hdt.util.io.compress.TripleGenerator;
import org.rdfhdt.hdt.util.listener.IntermediateListener;
import org.rdfhdt.hdt.util.listener.ListenerUtil;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;

/**
 * HDT Disk generation methods
 *
 * @author Antoine Willerval
 */
public class HDTDiskImporter implements Closeable {
	/**
	 * @return ram on the system
	 */
	public static long getAvailableMemory() {
		Runtime runtime = Runtime.getRuntime();
		return (runtime.maxMemory() - (runtime.totalMemory() - runtime.freeMemory()));
	}
	/**
	 * @return a theoretical maximum amount of memory the JVM will attempt to use
	 */
	static long getMaxChunkSize(int workers) {
		return (long) (getAvailableMemory() * 0.85 / (1.5 * 3 * workers));
	}

	// configs
	private final HDTOptions hdtFormat;
	private final MultiThreadListener listener;
	private final String compressMode;
	private final String futureHDTLocation;
	private final Path futureHDTLocationPath;
	private final CloseSuppressPath basePath;
	private final long chunkSize;
	private final int ways;
	private final int workers;
	private final int bufferSize;
	private final boolean mapHDT;
	private final boolean debugHDTBuilding;
	private final Profiler profiler;
	private final HDTBase<? extends HeaderPrivate, ? extends DictionaryPrivate, ? extends TriplesPrivate> hdt;
	private long rawSize;

	// component status
	private boolean dict = false;
	private boolean header = false;
	private boolean triples = false;

	public HDTDiskImporter(HDTOptions hdtFormat, ProgressListener progressListener, String baseURI) throws IOException {
		this.hdtFormat = hdtFormat;
		listener = ListenerUtil.multiThreadListener(progressListener);
		// load config

		// compression mode
		compressMode = hdtFormat.get(
				HDTOptionsKeys.LOADER_DISK_COMPRESSION_MODE_KEY,
				HDTOptionsKeys.LOADER_DISK_COMPRESSION_MODE_VALUE_COMPLETE
		); // see CompressionResult
		// worker for compression tasks
		workers = (int) hdtFormat.getInt(
				HDTOptionsKeys.LOADER_DISK_COMPRESSION_WORKER_KEY,
				Runtime.getRuntime()::availableProcessors
		);
		// check and set default values if required
		if (workers <= 0) {
			throw new IllegalArgumentException("Number of workers should be positive!");
		}
		// maximum size of a chunk
		chunkSize = hdtFormat.getInt(
				HDTOptionsKeys.LOADER_DISK_CHUNK_SIZE_KEY,
				() -> getMaxChunkSize(this.workers)
		);
		if (chunkSize < 0) {
			throw new IllegalArgumentException("Negative chunk size!");
		}
		long maxFileOpenedLong = hdtFormat.getInt(
				HDTOptionsKeys.LOADER_DISK_MAX_FILE_OPEN_KEY,
				1024
		);
		int maxFileOpened;
		if (maxFileOpenedLong < 0 || maxFileOpenedLong > Integer.MAX_VALUE) {
			throw new IllegalArgumentException("maxFileOpened should be positive!");
		} else {
			maxFileOpened = (int) maxFileOpenedLong;
		}
		long kwayLong = hdtFormat.getInt(
				HDTOptionsKeys.LOADER_DISK_KWAY_KEY,
				() -> Math.max(1, BitUtil.log2(maxFileOpened / this.workers))
		);
		if (kwayLong <= 0 || kwayLong > Integer.MAX_VALUE) {
			throw new IllegalArgumentException("kway can't be negative!");
		} else {
			ways = (int) kwayLong;
		}

		long bufferSizeLong = hdtFormat.getInt(HDTOptionsKeys.LOADER_DISK_BUFFER_SIZE_KEY, CloseSuppressPath.BUFFER_SIZE);
		if (bufferSizeLong > Integer.MAX_VALUE - 5L || bufferSizeLong <= 0) {
			throw new IllegalArgumentException("Buffer size can't be negative or bigger than the size of an array!");
		} else {
			bufferSize = (int) bufferSizeLong;
		}
		// location of the working directory, will be deleted after generation
		String baseNameOpt = hdtFormat.get(HDTOptionsKeys.LOADER_DISK_LOCATION_KEY);
		// location of the future HDT file, do not set to create the HDT in memory while mergin
		futureHDTLocation = hdtFormat.get(HDTOptionsKeys.LOADER_DISK_FUTURE_HDT_LOCATION_KEY);
		futureHDTLocationPath = futureHDTLocation == null ? null : Path.of(futureHDTLocation);

		profiler = Profiler.createOrLoadSubSection("doGenerateHDTDisk", hdtFormat, true);
		try {
			if (baseNameOpt == null || baseNameOpt.isEmpty()) {
				basePath = CloseSuppressPath.of(Files.createTempDirectory("hdt-java-generate-disk"));
			} else {
				basePath = CloseSuppressPath.of(baseNameOpt);
			}
			basePath.closeWithDeleteRecurse();
			mapHDT = futureHDTLocation != null && !futureHDTLocation.isEmpty();
			// debug the build strategy
			debugHDTBuilding = hdtFormat.getBoolean("debug.disk.build");

			// create working directory
			basePath.mkdirs();

			if (!mapHDT) {
				// using default implementation
				hdt = new HDTImpl(hdtFormat);
			} else {
				// using map implementation
				hdt = new WriteHDTImpl(hdtFormat, basePath.resolve("maphdt"), bufferSize);
			}
			hdt.setBaseUri(baseURI);
		} catch (Throwable t) {
			try {
				throw t;
			} finally {
				profiler.close();
			}
		}
	}

	/**
	 * create the Dictonary of the HDT
	 *
	 * @param iterator the triples stream to create the dictionary
	 * @throws java.lang.IllegalArgumentException is the component was already built
	 */
	public CompressTripleMapper compressDictionary(Iterator<TripleString> iterator) throws ParserException, IOException {
		if (this.dict) {
			throw new IllegalArgumentException("Dictionary already built! Use another importer instance!");
		}
		listener.notifyProgress(0, "Sorting sections with chunk of size: " + StringUtil.humanReadableByteCount(chunkSize, true) + "B with " + ways + "ways and " + workers + " worker(s)");

		AsyncIteratorFetcher<TripleString> source = new AsyncIteratorFetcher<>(iterator);

		profiler.pushSection("section compression");
		CompressionResult compressionResult;
		try {
			compressionResult = DictionaryFactory.createSectionCompressor(
					hdtFormat,
					basePath.resolve("sectionCompression"),
					source,
					listener,
					bufferSize,
					chunkSize * workers, 1 << ways,
					hdtFormat.getBoolean("debug.disk.slow.stream2")).compress(workers, compressMode);
		} catch (KWayMerger.KWayMergerException | InterruptedException e) {
			throw new ParserException(e);
		}
		CompressTripleMapper mapper;
		DictionaryPrivate dictionary = hdt.getDictionary();
		try {
			profiler.popSection();

			listener.unregisterAllThreads();
			listener.notifyProgress(20, "Create sections and triple mapping");

			profiler.pushSection("dictionary write");
			// create sections and triple mapping
			mapper = new CompressTripleMapper(basePath, compressionResult.getTripleCount(), chunkSize);
			try (CompressFourSectionDictionary modifiableDictionary = new CompressFourSectionDictionary(compressionResult, mapper, listener, debugHDTBuilding)) {
				dictionary.loadAsync(modifiableDictionary, listener);
			} catch (InterruptedException e) {
				throw new ParserException(e);
			}
			profiler.popSection();

			compressionResult.getMappedIdSubjects()
					.forEachRemaining(mappedId -> mapper.cloneSubject(mappedId.getTripleId(), mappedId.getMappedId()));
			compressionResult.getMappedIdPredicates()
					.forEachRemaining(mappedId -> mapper.clonePredicate(mappedId.getTripleId(), mappedId.getMappedId()));
			compressionResult.getMappedIdObjects()
					.forEachRemaining(mappedId -> mapper.cloneObject(mappedId.getTripleId(), mappedId.getMappedId()));

			// complete the mapper with the shared count and delete compression data
			rawSize = compressionResult.getRawSize();
		} finally {
			compressionResult.delete();
		}
		mapper.setShared(dictionary.getNshared());

		this.dict = true;
		return mapper;
	}

	/**
	 * create the Triples of the HDT
	 *
	 * @param mapper the mapper from the dictionary building
	 * @throws java.lang.IllegalArgumentException is the component was already built
	 */
	public void compressTriples(CompressTripleMapper mapper) throws ParserException, IOException {
		if (this.triples) {
			throw new IllegalArgumentException("Triples already built! Use another importer instance!");
		}
		listener.notifyProgress(40, "Create mapped and sort triple file");
		// create mapped triples file
		TripleCompressionResult tripleCompressionResult;
		TriplesPrivate triples = hdt.getTriples();
		TripleComponentOrder order = triples.getOrder();
		profiler.pushSection("triple compression/map");
		try {
			MapCompressTripleMerger tripleMapper = new MapCompressTripleMerger(
					basePath.resolve("tripleMapper"),
					new AsyncIteratorFetcher<>(new TripleGenerator(mapper.getTripleCount())),
					mapper,
					listener,
					order,
					bufferSize,
					chunkSize,
					1 << ways);
			tripleCompressionResult = tripleMapper.merge(workers, compressMode);
		} catch (KWayMerger.KWayMergerException | InterruptedException e) {
			throw new ParserException(e);
		}
		profiler.popSection();
		listener.unregisterAllThreads();

		profiler.pushSection("bit triple creation");
		try {
			// create bit triples and load the triples
			TempTriples tempTriples = tripleCompressionResult.getTriples();
			IntermediateListener il = new IntermediateListener(listener);
			il.setRange(80, 90);
			il.setPrefix("Create bit triples: ");
			il.notifyProgress(0, "create triples");
			triples.load(tempTriples, il);
			tempTriples.close();

			// completed the triples, delete the mapper
			mapper.delete();
		} finally {
			tripleCompressionResult.close();
		}
		profiler.popSection();

		this.triples = true;
	}

	/**
	 * create the Header of the HDT
	 *
	 * @throws java.lang.IllegalArgumentException is the component was already built
	 */
	public void createHeader() {
		if (this.header) {
			throw new IllegalArgumentException("Header already built! Use another importer instance!");
		}
		profiler.pushSection("header creation");

		listener.notifyProgress(90, "Create HDT header");
		// header
		hdt.populateHeaderStructure(hdt.getBaseURI());
		hdt.getHeader().insert("_:statistics", HDTVocabulary.ORIGINAL_SIZE, rawSize);

		profiler.popSection();

		this.header = true;
	}

	/**
	 * create the HDT from the parts of the previous methods
	 *
	 * @return hdt
	 * @throws IOException                        io exception during disk generation
	 * @throws java.lang.IllegalArgumentException is a component is missing
	 */
	public HDT convertToHDT() throws IOException {
		if (!this.dict) {
			throw new IllegalArgumentException("Dictionary missing, can't create HDT");
		}
		if (!this.triples) {
			throw new IllegalArgumentException("Triples missing, can't create HDT");
		}
		if (!this.header) {
			throw new IllegalArgumentException("Header missing, can't create HDT");
		}
		// return the HDT
		if (mapHDT) {
			profiler.pushSection("map to hdt");
			// write the HDT and map it
			try {
				hdt.saveToHDT(futureHDTLocation, listener);
			} finally {
				hdt.close();
			}
			IntermediateListener il = new IntermediateListener(listener);
			il.setPrefix("Map HDT: ");
			il.setRange(95, 100);
			il.notifyProgress(0, "start");
			try {
				return new MapOnCallHDT(futureHDTLocationPath);
			} finally {
				profiler.popSection();
			}
		} else {
			listener.notifyProgress(100, "HDT completed");
			return hdt;
		}
	}

	/**
	 * call all the step to create an HDT
	 *
	 * @param iterator the iterator to load the data
	 * @return hdt
	 * @throws IOException                        io exception during disk generation
	 * @throws ParserException                    parsing exception during disk generation
	 * @throws java.lang.IllegalArgumentException is a component is missing
	 */
	public HDT runAllSteps(Iterator<TripleString> iterator) throws IOException, ParserException {
		// compress the triples into sections and compressed triples
		CompressTripleMapper mapper = compressDictionary(iterator);
		compressTriples(mapper);
		createHeader();

		return convertToHDT();
	}

	@Override
	public void close() throws IOException {
		try {
			profiler.stop();
			profiler.writeProfiling();
			listener.notifyProgress(100, "Clearing disk");
		} finally {
			try {
				basePath.close();
			} finally {
				profiler.close();
			}
		}
	}
}
