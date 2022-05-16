package org.rdfhdt.hdt.hdt;

import org.rdfhdt.hdt.compact.bitmap.Bitmap;
import org.rdfhdt.hdt.dictionary.DictionaryPrivate;
import org.rdfhdt.hdt.dictionary.impl.CompressFourSectionDictionary;
import org.rdfhdt.hdt.dictionary.impl.MultipleSectionDictionary;
import org.rdfhdt.hdt.enums.CompressionType;
import org.rdfhdt.hdt.enums.RDFNotation;
import org.rdfhdt.hdt.enums.TripleComponentOrder;
import org.rdfhdt.hdt.exceptions.NotFoundException;
import org.rdfhdt.hdt.exceptions.ParserException;
import org.rdfhdt.hdt.hdt.impl.HDTBase;
import org.rdfhdt.hdt.hdt.impl.HDTImpl;
import org.rdfhdt.hdt.hdt.impl.TempHDTImporterOnePass;
import org.rdfhdt.hdt.hdt.impl.TempHDTImporterTwoPass;
import org.rdfhdt.hdt.hdt.impl.WriteHDTImpl;
import org.rdfhdt.hdt.hdt.impl.diskimport.CompressTripleMapper;
import org.rdfhdt.hdt.hdt.impl.diskimport.CompressionResult;
import org.rdfhdt.hdt.hdt.impl.diskimport.SectionCompressor;
import org.rdfhdt.hdt.hdt.impl.diskimport.TripleCompressionResult;
import org.rdfhdt.hdt.hdt.writer.TripleWriterHDT;
import org.rdfhdt.hdt.header.HeaderPrivate;
import org.rdfhdt.hdt.header.HeaderUtil;
import org.rdfhdt.hdt.iterator.utils.FileTripleIDIterator;
import org.rdfhdt.hdt.iterator.utils.FileTripleIterator;
import org.rdfhdt.hdt.listener.MultiThreadListener;
import org.rdfhdt.hdt.listener.ProgressListener;
import org.rdfhdt.hdt.options.HDTOptions;
import org.rdfhdt.hdt.options.HDTOptionsKeys;
import org.rdfhdt.hdt.options.HDTSpecification;
import org.rdfhdt.hdt.rdf.RDFParserCallback;
import org.rdfhdt.hdt.rdf.RDFParserFactory;
import org.rdfhdt.hdt.rdf.TripleWriter;
import org.rdfhdt.hdt.triples.TempTriples;
import org.rdfhdt.hdt.triples.TripleString;
import org.rdfhdt.hdt.triples.TriplesPrivate;
import org.rdfhdt.hdt.util.StopWatch;
import org.rdfhdt.hdt.util.concurrent.TreeWorker;
import org.rdfhdt.hdt.util.io.CloseSuppressPath;
import org.rdfhdt.hdt.util.io.IOUtil;
import org.rdfhdt.hdt.util.io.compress.CompressTripleReader;
import org.rdfhdt.hdt.util.io.compress.MapCompressTripleMerger;
import org.rdfhdt.hdt.util.listener.ListenerUtil;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.Iterator;

public class HDTManagerImpl extends HDTManager {

	private boolean useSimple(HDTOptions spec) {
		String value = spec.get("parser.ntSimpleParser");
		return value != null && !value.isEmpty() && !value.equals("false");
	}

	@Override
	public HDTOptions doReadOptions(String file) throws IOException {
		return new HDTSpecification(file);
	}

	@Override
	public HDT doLoadHDT(String hdtFileName, ProgressListener listener, HDTOptions spec) throws IOException {
		HDTPrivate hdt = new HDTImpl(spec);
		hdt.loadFromHDT(hdtFileName, listener);
		return hdt;
	}

	@Override
	protected HDT doMapHDT(String hdtFileName, ProgressListener listener, HDTOptions spec) throws IOException {
		HDTPrivate hdt = new HDTImpl(spec);
		hdt.mapFromHDT(new File(hdtFileName), 0, listener);
		return hdt;
	}


	@Override
	public HDT doLoadHDT(InputStream hdtFile, ProgressListener listener, HDTOptions spec) throws IOException {
		HDTPrivate hdt = new HDTImpl(spec);
		hdt.loadFromHDT(hdtFile, listener);
		return hdt;
	}

	@Override
	public HDT doLoadIndexedHDT(String hdtFileName, ProgressListener listener, HDTOptions spec) throws IOException {
		HDTPrivate hdt = new HDTImpl(spec);
		hdt.loadFromHDT(hdtFileName, listener);
		hdt.loadOrCreateIndex(listener);
		return hdt;
	}


	@Override
	public HDT doMapIndexedHDT(String hdtFileName, ProgressListener listener, HDTOptions spec) throws IOException {
		HDTPrivate hdt = new HDTImpl(spec);
		hdt.mapFromHDT(new File(hdtFileName), 0, listener);
		hdt.loadOrCreateIndex(listener);
		return hdt;
	}

	@Override
	public HDT doLoadIndexedHDT(InputStream hdtFile, ProgressListener listener, HDTOptions spec) throws IOException {
		HDTPrivate hdt = new HDTImpl(spec);
		hdt.loadFromHDT(hdtFile, listener);
		hdt.loadOrCreateIndex(listener);
		return hdt;
	}

	@Override
	public HDT doIndexedHDT(HDT hdt, ProgressListener listener) {
		((HDTPrivate) hdt).loadOrCreateIndex(listener);
		return hdt;
	}

	@Override
	public HDT doGenerateHDT(String rdfFileName, String baseURI, RDFNotation rdfNotation, HDTOptions spec, ProgressListener listener) throws IOException, ParserException {
		//choose the importer
		String loaderType = spec.get(HDTOptionsKeys.LOADER_TYPE_KEY);
		TempHDTImporter loader;
		if (HDTOptionsKeys.LOADER_TYPE_VALUE_TWO_PASS.equals(loaderType)) {
			loader = new TempHDTImporterTwoPass(useSimple(spec));
		} else {
			loader = new TempHDTImporterOnePass(useSimple(spec));
		}

		// Create TempHDT
		TempHDT modHdt = loader.loadFromRDF(spec, rdfFileName, baseURI, rdfNotation, listener);

		// Convert to HDT
		HDTImpl hdt = new HDTImpl(spec);
		hdt.loadFromModifiableHDT(modHdt, listener);
		hdt.populateHeaderStructure(modHdt.getBaseURI());

		// Add file size to Header
		try {
			long originalSize = HeaderUtil.getPropertyLong(modHdt.getHeader(), "_:statistics", HDTVocabulary.ORIGINAL_SIZE);
			hdt.getHeader().insert("_:statistics", HDTVocabulary.ORIGINAL_SIZE, originalSize);
		} catch (NotFoundException e) {
			// ignored
		}

		modHdt.close();

		return hdt;
	}

	@Override
	public HDT doGenerateHDT(InputStream fileStream, String baseURI, RDFNotation rdfNotation, CompressionType compressionType, HDTOptions hdtFormat, ProgressListener listener) throws IOException {
		// uncompress the stream if required
		fileStream = IOUtil.asUncompressed(fileStream, compressionType);
		// create a parser for this rdf stream
		RDFParserCallback parser = RDFParserFactory.getParserCallback(rdfNotation);
		// read the stream as triples
		Iterator<TripleString> iterator = RDFParserFactory.readAsIterator(parser, fileStream, baseURI, true, rdfNotation);

		return doGenerateHDT(iterator, baseURI, hdtFormat, listener);
	}

	@Override
	public HDT doGenerateHDT(Iterator<TripleString> triples, String baseURI, HDTOptions spec, ProgressListener listener) throws IOException {
		//choose the importer
		TempHDTImporterOnePass loader = new TempHDTImporterOnePass(false);

		// Create TempHDT
		TempHDT modHdt = loader.loadFromTriples(spec, triples, baseURI, listener);

		// Convert to HDT
		HDTImpl hdt = new HDTImpl(spec);
		hdt.loadFromModifiableHDT(modHdt, listener);
		hdt.populateHeaderStructure(modHdt.getBaseURI());

		// Add file size to Header
		try {
			long originalSize = HeaderUtil.getPropertyLong(modHdt.getHeader(), "_:statistics", HDTVocabulary.ORIGINAL_SIZE);
			hdt.getHeader().insert("_:statistics", HDTVocabulary.ORIGINAL_SIZE, originalSize);
		} catch (NotFoundException e) {
			// ignored
		}

		modHdt.close();

		return hdt;
	}

	@Override
	public HDT doGenerateHDTDisk(String rdfFileName, String baseURI, RDFNotation rdfNotation, CompressionType compressionType, HDTOptions hdtFormat, ProgressListener listener) throws IOException, ParserException {
		// read this file as stream, do not compress to allow the compressionType to be different from the file extension
		try (InputStream stream = IOUtil.getFileInputStream(rdfFileName, false)) {
			return doGenerateHDTDisk(stream, baseURI, rdfNotation, compressionType, hdtFormat, listener);
		}
	}

	@Override
	public HDT doGenerateHDTDisk(InputStream fileStream, String baseURI, RDFNotation rdfNotation, CompressionType compressionType, HDTOptions hdtFormat, ProgressListener listener) throws IOException, ParserException {
		// uncompress the stream if required
		fileStream = IOUtil.asUncompressed(fileStream, compressionType);
		// create a parser for this rdf stream
		RDFParserCallback parser = RDFParserFactory.getParserCallback(rdfNotation);
		// read the stream as triples
		Iterator<TripleString> iterator = RDFParserFactory.readAsIterator(parser, fileStream, baseURI, true, rdfNotation);

		return doGenerateHDTDisk(iterator, baseURI, hdtFormat, listener);
	}

	/**
	 * @return a theoretical maximum amount of memory the JVM will attempt to use
	 */
	static long getMaxChunkSize(int processors) {
		Runtime runtime = Runtime.getRuntime();
		long presFreeMemory = (long) ((runtime.maxMemory() - (runtime.totalMemory() - runtime.freeMemory())) * 0.85);
		return presFreeMemory / processors;
	}

	@Override
	public HDT doGenerateHDTDisk(Iterator<TripleString> iterator, String baseURI, HDTOptions hdtFormat, ProgressListener progressListener) throws IOException, ParserException {
		MultiThreadListener listener = ListenerUtil.multiThreadListener(progressListener);
		// load config
		// compression mode
		String compressMode = hdtFormat.get(HDTOptionsKeys.LOADER_DISK_COMPRESSION_MODE_KEY); // see CompressionResult
		// worker for compression tasks
		int workers = (int) hdtFormat.getInt(HDTOptionsKeys.LOADER_DISK_COMPRESSION_WORKER_KEY);
		// maximum size of a chunk
		long chunkSize = hdtFormat.getInt(HDTOptionsKeys.LOADER_DISK_CHUNK_SIZE_KEY);
		// location of the working directory, will be deleted after generation
		String baseNameOpt = hdtFormat.get(HDTOptionsKeys.LOADER_DISK_LOCATION_KEY);
		CloseSuppressPath basePath;
		// location of the future HDT file, do not set to create the HDT in memory while mergin
		String futureHDTLocation = hdtFormat.get(HDTOptionsKeys.LOADER_DISK_FUTURE_HDT_LOCATION_KEY);

		// check and set default values if required
		if (workers == 0) {
			workers = Runtime.getRuntime().availableProcessors();
		} else if (workers < 0) {
			throw new IllegalArgumentException("Negative number of workers!");
		}
		if (baseNameOpt == null || baseNameOpt.isEmpty()) {
			basePath = CloseSuppressPath.of(Files.createTempDirectory("hdt-java-generate-disk"));
		} else {
			basePath = CloseSuppressPath.of(baseNameOpt);
		}
		basePath.closeWithDeleteRecurse();
		if (chunkSize == 0) {
			chunkSize = getMaxChunkSize(workers);
		} else if (chunkSize < 0) {
			throw new IllegalArgumentException("Negative chunk size!");
		}
		boolean mapHDT = futureHDTLocation != null && !futureHDTLocation.isEmpty();

		// create working directory
		basePath.mkdirs();
		try {
			// compress the triples into sections and compressed triples
			listener.notifyProgress(0, "Sorting sections");

			FileTripleIterator triplesFile = new FileTripleIterator(iterator, chunkSize / 3);

			CompressionResult compressionResult;
			try (SectionCompressor sectionCompressor = new SectionCompressor(basePath, triplesFile, listener)) {
				compressionResult = sectionCompressor.compress(workers, compressMode);
			} catch (TreeWorker.TreeWorkerException | InterruptedException e) {
				throw new ParserException(e);
			}
			HDTBase<? extends HeaderPrivate, ? extends DictionaryPrivate, ? extends TriplesPrivate> hdt;
			if (!mapHDT) {
				// using default implementation
				hdt = new HDTImpl(hdtFormat);
			} else {
				// using map implementation
				hdt = new WriteHDTImpl(hdtFormat, basePath.resolve("maphdt"));
			}
			hdt.setBaseUri(baseURI);

			listener.unregisterAllThreads();
			listener.notifyProgress(20, "Create sections and triple mapping");

			// create sections and triple mapping
			DictionaryPrivate dictionary = hdt.getDictionary();
			CompressTripleMapper mapper = new CompressTripleMapper(basePath, compressionResult.getTripleCount());
			CompressFourSectionDictionary modifiableDictionary = new CompressFourSectionDictionary(compressionResult, mapper);
			try {
				dictionary.loadAsync(modifiableDictionary, listener);
			} catch (InterruptedException e) {
				throw new ParserException(e);
			}

			// complete the mapper with the shared count and delete compression data
			compressionResult.delete();
			mapper.setShared(dictionary.getNshared());

			listener.notifyProgress(40, "Create mapped and sort triple file");
			// create mapped triples file
			TripleCompressionResult tripleCompressionResult;
			TriplesPrivate triples = hdt.getTriples();
			TripleComponentOrder order = triples.getOrder();
			try (CompressTripleReader tripleReader = new CompressTripleReader(compressionResult.getTriples().openInputStream(true))) {
				MapCompressTripleMerger tripleMapper = new MapCompressTripleMerger(basePath, new FileTripleIDIterator(tripleReader.asIterator(), chunkSize), mapper, listener, order);
				tripleCompressionResult = tripleMapper.merge(workers, compressMode);
			} catch (TreeWorker.TreeWorkerException | InterruptedException e) {
				throw new ParserException(e);
			}
			listener.unregisterAllThreads();

			try {
				listener.notifyProgress(80, "Create bit triples");
				// create bit triples and load the triples
				TempTriples tempTriples = tripleCompressionResult.getTriples();
				triples.load(tempTriples, listener);
				tempTriples.close();

				// completed the triples, delete the mapper
				mapper.delete();
			} finally {
				tripleCompressionResult.close();
			}

			listener.notifyProgress(90, "Create HDT header");
			// header
			hdt.populateHeaderStructure(hdt.getBaseURI());
			hdt.getHeader().insert("_:statistics", HDTVocabulary.ORIGINAL_SIZE, triplesFile.getTotalSize());

			// return the HDT
			if (mapHDT) {
				// write the HDT and map it
				try {
					hdt.saveToHDT(futureHDTLocation, listener);
				} finally {
					hdt.close();
				}
				listener.notifyProgress(100, "Map HDT");
				return doMapHDT(futureHDTLocation, listener, hdtFormat);
			} else {
				listener.notifyProgress(100, "HDT completed");
				return hdt;
			}
		} finally {
			listener.notifyProgress(100, "Clearing disk");
			basePath.close();
		}
	}

	@Override
	protected TripleWriter doGetHDTWriter(OutputStream out, String baseURI, HDTOptions hdtFormat) {
		return new TripleWriterHDT(baseURI, hdtFormat, out);
	}

	@Override
	protected TripleWriter doGetHDTWriter(String outFile, String baseURI, HDTOptions hdtFormat) throws IOException {
		return new TripleWriterHDT(baseURI, hdtFormat, outFile, false);
	}

	@Override
	public HDT doHDTCat(String location, String hdtFileName1, String hdtFileName2, HDTOptions hdtFormat, ProgressListener listener) throws IOException {
		StopWatch st = new StopWatch();
		HDT hdt1 = doMapHDT(hdtFileName1, listener, hdtFormat);
		HDT hdt2 = doMapHDT(hdtFileName2, listener, hdtFormat);
		HDTImpl hdt = new HDTImpl(hdtFormat);
		if (hdt1.getDictionary() instanceof MultipleSectionDictionary
				&& hdt2.getDictionary() instanceof MultipleSectionDictionary)
			hdt.catCustom(location, hdt1, hdt2, listener);
		else
			hdt.cat(location, hdt1, hdt2, listener);
		return hdt;
	}

	@Override
	public HDT doHDTDiff(String hdtFileName1, String hdtFileName2, HDTOptions hdtFormat, ProgressListener listener) throws IOException {
		HDT hdt1 = doMapHDT(hdtFileName1, listener, hdtFormat);
		HDT hdt2 = doMapHDT(hdtFileName2, listener, hdtFormat);
		HDTImpl hdt = new HDTImpl(hdtFormat);
		hdt.diff(hdt1, hdt2, listener);
		return hdt;
	}

	@Override
	protected HDT doHDTDiffBit(String location, String hdtFileName, Bitmap deleteBitmap, HDTOptions hdtFormat, ProgressListener listener) throws IOException {
		HDT hdtOriginal = doMapHDT(hdtFileName, listener, hdtFormat);
		HDTImpl hdt = new HDTImpl(hdtFormat);
		hdt.diffBit(location, hdtOriginal, deleteBitmap, listener);
		return hdt;
	}
}