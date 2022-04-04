package org.rdfhdt.hdt.hdt;

import org.rdfhdt.hdt.compact.bitmap.Bitmap;
import org.rdfhdt.hdt.dictionary.impl.MultipleSectionDictionary;
import org.rdfhdt.hdt.enums.CompressionType;
import org.rdfhdt.hdt.enums.RDFNotation;
import org.rdfhdt.hdt.exceptions.NotFoundException;
import org.rdfhdt.hdt.exceptions.ParserException;
import org.rdfhdt.hdt.hdt.impl.HDTImpl;
import org.rdfhdt.hdt.hdt.impl.TempHDTImporterOnePass;
import org.rdfhdt.hdt.hdt.impl.TempHDTImporterTwoPass;
import org.rdfhdt.hdt.hdt.impl.TreeWorkerGenerateDisk;
import org.rdfhdt.hdt.hdt.writer.TripleWriterHDT;
import org.rdfhdt.hdt.header.HeaderUtil;
import org.rdfhdt.hdt.listener.ProgressListener;
import org.rdfhdt.hdt.options.HDTOptions;
import org.rdfhdt.hdt.options.HDTSpecification;
import org.rdfhdt.hdt.rdf.RDFParserCallback;
import org.rdfhdt.hdt.rdf.RDFParserFactory;
import org.rdfhdt.hdt.rdf.TripleWriter;
import org.rdfhdt.hdt.triples.TripleString;
import org.rdfhdt.hdt.util.StopWatch;
import org.rdfhdt.hdt.util.io.IOUtil;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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
		((HDTPrivate)hdt).loadOrCreateIndex(listener);
		return hdt;
	}

	@Override
	public HDT doGenerateHDT(String rdfFileName, String baseURI, RDFNotation rdfNotation, HDTOptions spec, ProgressListener listener) throws IOException, ParserException {
		//choose the importer
		String loaderType = spec.get("loader.type");
		TempHDTImporter loader;
		if ("two-pass".equals(loaderType)) {
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
		}
		
		modHdt.close();
		
		return hdt;
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
		Iterator<TripleString> iterator = RDFParserFactory.readAsIterator(parser, fileStream, baseURI, rdfNotation);

		return doGenerateHDTDisk(iterator, baseURI, hdtFormat, listener);
	}

	@Override
	public HDT doGenerateHDTDisk(Iterator<TripleString> iterator, String baseURI, HDTOptions hdtFormat, ProgressListener listener) throws IOException, ParserException {
		return TreeWorkerGenerateDisk.generateHDT(iterator, hdtFormat, listener);
	}

	@Override
	protected TripleWriter doGetHDTWriter(OutputStream out, String baseURI, HDTOptions hdtFormat) throws IOException {
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
		if(hdt1.getDictionary() instanceof MultipleSectionDictionary
				&& hdt2.getDictionary() instanceof MultipleSectionDictionary)
			hdt.catCustom(location,hdt1,hdt2,listener);
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