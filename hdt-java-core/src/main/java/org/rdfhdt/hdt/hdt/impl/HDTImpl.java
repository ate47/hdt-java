/**
 * File: $HeadURL: https://hdt-java.googlecode.com/svn/trunk/hdt-java/src/org/rdfhdt/hdt/hdt/impl/HDTImpl.java $
 * Revision: $Rev: 202 $
 * Last modified: $Date: 2013-05-10 18:04:41 +0100 (vie, 10 may 2013) $
 * Last modified by: $Author: mario.arias $
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation;
 * version 3.0 of the License.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
 *
 * Contacting the authors:
 *   Mario Arias:               mario.arias@deri.org
 *   Javier D. Fernandez:       jfergar@infor.uva.es
 *   Miguel A. Martinez-Prieto: migumar2@infor.uva.es
 *   Alejandro Andres:          fuzzy.alej@gmail.com
 *   Dennis Diefenbach:         dennis.diefenbach@univ-st-etienne.fr
 */

package org.rdfhdt.hdt.hdt.impl;

import org.rdfhdt.hdt.compact.bitmap.Bitmap;
import org.rdfhdt.hdt.compact.bitmap.BitmapFactory;
import org.rdfhdt.hdt.compact.bitmap.ModifiableBitmap;
import org.rdfhdt.hdt.dictionary.DictionaryCat;
import org.rdfhdt.hdt.dictionary.DictionaryDiff;
import org.rdfhdt.hdt.dictionary.DictionaryFactory;
import org.rdfhdt.hdt.dictionary.DictionaryPrivate;
import org.rdfhdt.hdt.dictionary.DictionarySection;
import org.rdfhdt.hdt.dictionary.TempDictionary;
import org.rdfhdt.hdt.dictionary.impl.FourSectionDictionary;
import org.rdfhdt.hdt.dictionary.impl.FourSectionDictionaryBig;
import org.rdfhdt.hdt.dictionary.impl.FourSectionDictionaryCat;
import org.rdfhdt.hdt.dictionary.impl.MultipleSectionDictionary;
import org.rdfhdt.hdt.dictionary.impl.MultipleSectionDictionaryBig;
import org.rdfhdt.hdt.dictionary.impl.MultipleSectionDictionaryCat;
import org.rdfhdt.hdt.enums.ResultEstimationType;
import org.rdfhdt.hdt.enums.TripleComponentRole;
import org.rdfhdt.hdt.exceptions.IllegalFormatException;
import org.rdfhdt.hdt.exceptions.NotFoundException;
import org.rdfhdt.hdt.exceptions.NotImplementedException;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTVersion;
import org.rdfhdt.hdt.hdt.HDTVocabulary;
import org.rdfhdt.hdt.hdt.TempHDT;
import org.rdfhdt.hdt.header.HeaderFactory;
import org.rdfhdt.hdt.header.HeaderPrivate;
import org.rdfhdt.hdt.iterator.DictionaryTranslateIterator;
import org.rdfhdt.hdt.iterator.DictionaryTranslateIteratorBuffer;
import org.rdfhdt.hdt.listener.ProgressListener;
import org.rdfhdt.hdt.options.ControlInfo;
import org.rdfhdt.hdt.options.ControlInformation;
import org.rdfhdt.hdt.options.HDTOptions;
import org.rdfhdt.hdt.options.HDTSpecification;
import org.rdfhdt.hdt.triples.DictionaryEntriesDiff;
import org.rdfhdt.hdt.triples.IteratorTripleID;
import org.rdfhdt.hdt.triples.IteratorTripleString;
import org.rdfhdt.hdt.triples.TempTriples;
import org.rdfhdt.hdt.triples.TripleID;
import org.rdfhdt.hdt.triples.TripleString;
import org.rdfhdt.hdt.triples.TriplesFactory;
import org.rdfhdt.hdt.triples.TriplesPrivate;
import org.rdfhdt.hdt.triples.impl.BitmapTriples;
import org.rdfhdt.hdt.triples.impl.BitmapTriplesCat;
import org.rdfhdt.hdt.triples.impl.BitmapTriplesIteratorCat;
import org.rdfhdt.hdt.triples.impl.BitmapTriplesIteratorDiff;
import org.rdfhdt.hdt.triples.impl.BitmapTriplesIteratorMapDiff;
import org.rdfhdt.hdt.util.StopWatch;
import org.rdfhdt.hdt.util.io.CountInputStream;
import org.rdfhdt.hdt.util.io.IOUtil;
import org.rdfhdt.hdt.util.listener.IntermediateListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.Map;
import java.util.zip.GZIPInputStream;

/**
 * Basic implementation of HDT interface
 *
 */
public class HDTImpl extends HDTBase<HeaderPrivate, DictionaryPrivate, TriplesPrivate> {
	private static final Logger log = LoggerFactory.getLogger(HDTImpl.class);

	private String hdtFileName;
	private String baseUri;
	private boolean isMapped;
	private boolean isClosed=false;

	public HDTImpl(HDTOptions spec) {
		super(spec);

		header = HeaderFactory.createHeader(this.spec);
		dictionary = DictionaryFactory.createDictionary(this.spec);
		triples = TriplesFactory.createTriples(this.spec);
	}

	@Override
	public void loadFromHDT(InputStream input, ProgressListener listener) throws IOException {
		ControlInfo ci = new ControlInformation();
		IntermediateListener iListener = new IntermediateListener(listener);

		// Load Global ControlInformation
		ci.clear();
		ci.load(input);
		String hdtFormat = ci.getFormat();
		if(!hdtFormat.equals(HDTVocabulary.HDT_CONTAINER) && !hdtFormat.equals(HDTVocabulary.HDT_CONTAINER_2)) {
			throw new IllegalFormatException("This software (v" + HDTVersion.HDT_VERSION + ".x.x | v"+HDTVersion.HDT_VERSION_2+".x.x) cannot open this version of HDT File (" + hdtFormat + ")");
		}

		// Load header
		ci.clear();
		ci.load(input);
		iListener.setRange(0, 5);
		header = HeaderFactory.createHeader(ci);
		header.load(input, ci, iListener);

		// Set base URI.
		this.baseUri = header.getBaseURI().toString();

		// Load dictionary
		ci.clear();
		ci.load(input);
		iListener.setRange(5, 60);
		dictionary = DictionaryFactory.createDictionary(ci);
		dictionary.load(input, ci, iListener);

		// Load Triples
		ci.clear();
		ci.load(input);
		iListener.setRange(60, 100);
		triples = TriplesFactory.createTriples(ci);
		triples.load(input, ci, iListener);
		
		isClosed=false;
	}

	@Override
	public void loadFromHDT(String hdtFileName, ProgressListener listener)	throws IOException {
		InputStream in;
		if(hdtFileName.endsWith(".gz")) {
			in = new BufferedInputStream(new GZIPInputStream(new FileInputStream(hdtFileName)));
		} else {
			in = new CountInputStream(new BufferedInputStream(new FileInputStream(hdtFileName)));
		}
		loadFromHDT(in, listener);
		in.close();

		this.hdtFileName = hdtFileName;
		
		isClosed=false;
	}

	@Override
	public void mapFromHDT(File f, long offset, ProgressListener listener) throws IOException {
		this.hdtFileName = f.toString();
		this.isMapped = true;

		CountInputStream input;
		if(hdtFileName.endsWith(".gz")) {
			File old = f;
			hdtFileName = hdtFileName.substring(0, hdtFileName.length()-3);
			f = new File(hdtFileName);

			if(!f.exists()) {
				System.err.println("We cannot map a gzipped HDT, decompressing into "+hdtFileName+" first.");
				IOUtil.decompressGzip(old, f);
				System.err.println("Gzipped HDT successfully decompressed. You might want to delete "+old.getAbsolutePath()+" to save disk space.");
			} else {
				System.err.println("We cannot map a gzipped HDT, using "+hdtFileName+" instead.");
			}
		}

		input = new CountInputStream(new BufferedInputStream(new FileInputStream(hdtFileName)));

		ControlInfo ci = new ControlInformation();
		IntermediateListener iListener = new IntermediateListener(listener);

		// Load Global ControlInformation
		ci.clear();
		ci.load(input);
		String hdtFormat = ci.getFormat();
		if(!hdtFormat.equals(HDTVocabulary.HDT_CONTAINER) && !hdtFormat.equals(HDTVocabulary.HDT_CONTAINER_2)) {
			throw new IllegalFormatException("This software (v" + HDTVersion.HDT_VERSION + ".x.x | v"+HDTVersion.HDT_VERSION_2+".x.x) cannot open this version of HDT File (" + hdtFormat + ")");
		}

		// Load header
		ci.clear();
		ci.load(input);
		iListener.setRange(0, 5);
		header = HeaderFactory.createHeader(ci);
		header.load(input, ci, iListener);

		// Set base URI.
		this.baseUri = header.getBaseURI().toString();

		// Load dictionary
		ci.clear();
		input.mark(1024);
		ci.load(input);
		input.reset();
		iListener.setRange(5, 60);
		dictionary = DictionaryFactory.createDictionary(ci);
		dictionary.mapFromFile(input, f, iListener);

		// Load Triples
		ci.clear();
		input.mark(1024);
		ci.load(input);
		input.reset();
		iListener.setRange(60, 100);
		triples = TriplesFactory.createTriples(ci);
		triples.mapFromFile(input, f, iListener);

		// Close the file used to keep track of positions.
		input.close();
		
		isClosed=false;
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see hdt.HDT#saveToHDT(java.io.OutputStream)
	 */
	@Override
	public void saveToHDT(String fileName, ProgressListener listener) throws IOException {
		OutputStream out = new BufferedOutputStream(new FileOutputStream(fileName));
		//OutputStream out = new GZIPOutputStream(new BufferedOutputStream(new FileOutputStream(fileName)));
		saveToHDT(out, listener);
		out.close();

		this.hdtFileName = fileName;
	}

	@Override
	public IteratorTripleString search(CharSequence subject, CharSequence predicate, CharSequence object) throws NotFoundException {

		if(isClosed) {
			throw new IllegalStateException("Cannot search an already closed HDT");
		}
		
		// Conversion from TripleString to TripleID
		TripleID triple = new TripleID(
				dictionary.stringToId(subject, TripleComponentRole.SUBJECT),
				dictionary.stringToId(predicate, TripleComponentRole.PREDICATE),
				dictionary.stringToId(object, TripleComponentRole.OBJECT)
			);

		if(triple.isNoMatch()) {
			//throw new NotFoundException("String not found in dictionary");
			return new IteratorTripleString() {
				@Override
				public TripleString next() {
					return null;
				}
				@Override
				public boolean hasNext() {
					return false;
				}
				@Override
				public ResultEstimationType numResultEstimation() {
					return ResultEstimationType.EXACT;
				}
				@Override
				public void goToStart() {
				}
				@Override
				public long estimatedNumResults() {
					return 0;
				}

				@Override
				public long getLastTriplePosition() {
					throw new NotImplementedException();
				}
			};
		}

		if(isMapped) {
			try {
				if(dictionary instanceof MultipleSectionDictionary){
					return new DictionaryTranslateIteratorBuffer(triples.search(triple), (MultipleSectionDictionary) dictionary, subject, predicate, object);
				}else{
					return new DictionaryTranslateIteratorBuffer(triples.search(triple), (FourSectionDictionary) dictionary, subject, predicate, object);

				}
			}catch(NullPointerException e) {
				e.printStackTrace();
				return new DictionaryTranslateIterator(triples.search(triple), dictionary, subject, predicate, object);
			}
		} else {
			return new DictionaryTranslateIterator(triples.search(triple), dictionary, subject, predicate, object);
		}
	}

	public void loadFromParts(HeaderPrivate h, DictionaryPrivate d, TriplesPrivate t) {
		this.header = h;
		this.dictionary = d;
		this.triples = t;
		isClosed=false;
	}

	@Override
	public void setBaseUri(String baseUri) {
		this.baseUri = baseUri;
	}

	public void loadFromModifiableHDT(TempHDT modHdt, ProgressListener listener) {

		modHdt.reorganizeDictionary(listener);
		modHdt.reorganizeTriples(listener);

        // Get parts
        TempTriples modifiableTriples = modHdt.getTriples();
        TempDictionary modifiableDictionary = modHdt.getDictionary();

        // Convert triples to final format
        if(triples.getClass().equals(modifiableTriples.getClass())) {
                triples = modifiableTriples;
        } else {
        		//StopWatch tripleConvTime = new StopWatch();
                triples.load(modifiableTriples, listener);
                //System.out.println("Triples conversion time: "+tripleConvTime.stopAndShow());
        }

        // Convert dictionary to final format
        if(dictionary.getClass().equals(modifiableDictionary.getClass())) {
                dictionary = (DictionaryPrivate)modifiableDictionary;
        } else {
                //StopWatch dictConvTime = new StopWatch();
                dictionary.load(modifiableDictionary, listener);
                //System.out.println("Dictionary conversion time: "+dictConvTime.stopAndShow());
        }

        this.baseUri = modHdt.getBaseURI();
        isClosed=false;
	}

	/* (non-Javadoc)
	 * @see hdt.hdt.HDT#generateIndex(hdt.listener.ProgressListener)
	 */
	@Override
	public void loadOrCreateIndex(ProgressListener listener) {
		if(triples.getNumberOfElements()==0) {
			// We need no index.
			return;
		}
		ControlInfo ci = new ControlInformation();
		String indexName = hdtFileName+ HDTVersion.get_index_suffix("-");
		indexName = indexName.replaceAll("\\.hdt\\.gz", "hdt");
		String versionName = indexName;
		File ff = new File(indexName);
		// backward compatibility
		if (!ff.isFile() || !ff.canRead()){
			indexName = hdtFileName+ (".index");
			indexName = indexName.replaceAll("\\.hdt\\.gz", "hdt");
			ff = new File(indexName);
		}
		CountInputStream in=null;
		try {
			in = new CountInputStream(new BufferedInputStream(new FileInputStream(ff)));
			ci.load(in);
			if(isMapped) {
				triples.mapIndex(in, new File(indexName), ci, listener);
			} else {
				triples.loadIndex(in, ci, listener);
			}
		} catch (Exception e) {
			if (!(e instanceof FileNotFoundException)) {
				System.out.println("Error reading .hdt.index, generating a new one. The error was: "+e.getMessage());
				e.printStackTrace();
			}

			// GENERATE
			StopWatch st = new StopWatch();
			triples.generateIndex(listener);

			// SAVE
			if(this.hdtFileName!=null) {
				BufferedOutputStream out=null;
				try {
					out = new BufferedOutputStream(new FileOutputStream(versionName));
					ci.clear();
					triples.saveIndex(out, ci, listener);
					out.close();
					System.out.println("Index generated and saved in "+st.stopAndShow());
				} catch (IOException e2) {
					System.err.println("Error writing index file.");
					e2.printStackTrace();
				} finally {
					IOUtil.closeQuietly(out);
				}
			}
		} finally {
			IOUtil.closeQuietly(in);
		}
	}

	@Override
	public String getBaseURI() {
		return baseUri;
	}

	protected void setTriples(TriplesPrivate triples) {
		this.triples = triples;
	}

	@Override
	public void close() throws IOException {
		if (isClosed) {
			return;
		}
		isClosed=true;
		dictionary.close();
		triples.close();
	}
	
	// For debugging
	@Override
	public String toString() {
		return String.format("HDT[file=%s,#triples=%d]", hdtFileName, triples.getNumberOfElements());
	}
	
	public String getHDTFileName() {
		return hdtFileName;
	}

	@Override
	public boolean isClosed() {
		return isClosed;
	}

	public boolean isMapped() {
		return isMapped;
	}

    /**
     * Merges two hdt files hdt1 and hdt2 on disk at location
     * @param location
     * @param hdt1
     * @param hdt2
     * @param listener
     */
	public void cat(String location, HDT hdt1, HDT hdt2, ProgressListener listener) throws IOException {
		try {
			System.out.println("Generating dictionary");
			FourSectionDictionaryCat dictionaryCat = new FourSectionDictionaryCat(location);
			dictionaryCat.cat(hdt1.getDictionary(),hdt2.getDictionary(), listener);
			//map the generated dictionary
			ControlInfo ci2 = new ControlInformation();
			CountInputStream fis = new CountInputStream(new BufferedInputStream(new FileInputStream(location + "dictionary")));
			FourSectionDictionaryBig dictionary = new FourSectionDictionaryBig(new HDTSpecification());
			fis.mark(1024);
			ci2.load(fis);
			fis.reset();
			dictionary.mapFromFile(fis, new File(location + "dictionary"),null);
			this.dictionary = dictionary;
			System.out.println("Generating triples");
			BitmapTriplesIteratorCat it = new BitmapTriplesIteratorCat(hdt1.getTriples(),hdt2.getTriples(),dictionaryCat);
			BitmapTriplesCat bitmapTriplesCat = new BitmapTriplesCat(location);
			bitmapTriplesCat.cat(it,listener);
			dictionaryCat.closeAll();
			//Delete the mappings since they are not necessary anymore
			try {
				Files.delete( Paths.get(location+"P1"));
				Files.delete( Paths.get(location+"P1"+"Types"));
				Files.delete( Paths.get(location+"P2"));
				Files.delete( Paths.get(location+"P2"+"Types"));
				Files.delete( Paths.get(location+"SH1"));
				Files.delete( Paths.get(location+"SH1"+"Types"));
				Files.delete( Paths.get(location+"SH2"));
				Files.delete( Paths.get(location+"SH2"+"Types"));
				Files.delete( Paths.get(location+"S1"));
				Files.delete( Paths.get(location+"S1"+"Types"));
				Files.delete( Paths.get(location+"S2"));
				Files.delete( Paths.get(location+"S2"+"Types"));
				Files.delete( Paths.get(location+"O1"));
				Files.delete( Paths.get(location+"O1"+"Types"));
				Files.delete( Paths.get(location+"O2"));
				Files.delete( Paths.get(location+"O2"+"Types"));
			} catch (Exception e) {
				// swallow this exception intentionally. See javadoc on Files.delete for details.
			}

			//map the triples
			CountInputStream fis2 = new CountInputStream(new BufferedInputStream(new FileInputStream(location + "triples")));
			ci2 = new ControlInformation();
			ci2.clear();
			fis2.mark(1024);
			ci2.load(fis2);
			fis2.reset();
			triples = TriplesFactory.createTriples(ci2);
			triples.mapFromFile(fis2,new File(location + "triples"),null);
			try {
				Files.delete(Paths.get(location + "mapping_back_1"));
				Files.delete(Paths.get(location + "mapping_back_2"));
				Files.delete(Paths.get(location + "mapping_back_type_1"));
				Files.delete(Paths.get(location + "mapping_back_type_2"));
			} catch (Exception e) {
				// swallow this exception intentionally. See javadoc on Files.delete for details.
			}
			System.out.println("Generating header");
			this.header = HeaderFactory.createHeader(spec);
			this.populateHeaderStructure("http://wdaqua.eu/hdtCat/");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void catCustom(String location, HDT hdt1, HDT hdt2, ProgressListener listener) throws IOException {
		System.out.println("Generating dictionary");
		DictionaryCat dictionaryCat = new MultipleSectionDictionaryCat(location);
		dictionaryCat.cat(hdt1.getDictionary(),hdt2.getDictionary(), listener);
		//map the generated dictionary
		ControlInfo ci2 = new ControlInformation();
		CountInputStream fis = new CountInputStream(new BufferedInputStream(new FileInputStream(location + "dictionary")));
		HDTSpecification spec = new HDTSpecification();
		spec.setOptions("tempDictionary.impl=multHash;dictionary.type=dictionaryMultiObj;");
		MultipleSectionDictionaryBig dictionary = new MultipleSectionDictionaryBig(spec);
		fis.mark(1024);
		ci2.load(fis);
		fis.reset();
		dictionary.mapFromFile(fis, new File(location + "dictionary"),null);
		this.dictionary = dictionary;

		System.out.println("Generating triples");
		BitmapTriplesIteratorCat it = new BitmapTriplesIteratorCat(hdt1.getTriples(),hdt2.getTriples(),dictionaryCat);
		BitmapTriplesCat bitmapTriplesCat = new BitmapTriplesCat(location);
		bitmapTriplesCat.cat(it,listener);
		//Delete the mappings since they are not necessary anymore
		Iterator<Map.Entry<String, DictionarySection>> iter = hdt1.getDictionary().getAllObjects().entrySet().iterator();
		int countSubSections = 0;
		while (iter.hasNext()){
			Map.Entry<String, DictionarySection> entry = iter.next();
			String dataType = entry.getKey();
			String prefix = "sub"+countSubSections;
			if(dataType.equals("NO_DATATYPE"))
				prefix = dataType;
			Files.delete(Paths.get(location+prefix+"1"));
			Files.delete(Paths.get(location+prefix+"1"+"Types"));
			countSubSections++;
		}
		iter = hdt2.getDictionary().getAllObjects().entrySet().iterator();
		countSubSections = 0;
		while (iter.hasNext()){
			Map.Entry<String, DictionarySection> entry = iter.next();
			String dataType = entry.getKey();
			String prefix = "sub"+countSubSections;
			if(dataType.equals("NO_DATATYPE"))
				prefix = dataType;
			Files.delete(Paths.get(location+prefix+"2"));
			Files.delete(Paths.get(location+prefix+"2"+"Types"));
			countSubSections++;
		}
		Files.delete(Paths.get(location+"P1"));
		Files.delete(Paths.get(location+"P1"+"Types"));
		Files.delete(Paths.get(location+"P2"));
		Files.delete(Paths.get(location+"P2"+"Types"));
		Files.delete(Paths.get(location+"SH1"));
		Files.delete(Paths.get(location+"SH1"+"Types"));
		Files.delete(Paths.get(location+"SH2"));
		Files.delete(Paths.get(location+"SH2"+"Types"));
		Files.delete(Paths.get(location+"S1"));
		Files.delete(Paths.get(location+"S1"+"Types"));
		Files.delete(Paths.get(location+"S2"));
		Files.delete(Paths.get(location+"S2"+"Types"));
		Files.delete(Paths.get(location+"O1"));
		Files.delete(Paths.get(location+"O1"+"Types"));
		Files.delete(Paths.get(location+"O2"));
		Files.delete(Paths.get(location+"O2"+"Types"));
		//map the triples
		CountInputStream fis2 = new CountInputStream(new BufferedInputStream(new FileInputStream(location + "triples")));
		ci2 = new ControlInformation();
		ci2.clear();
		fis2.mark(1024);
		ci2.load(fis2);
		fis2.reset();
		triples = TriplesFactory.createTriples(ci2);
		triples.mapFromFile(fis2,new File(location + "triples"),null);
		Files.delete(Paths.get(location+"mapping_back_1"));
		Files.delete(Paths.get(location+"mapping_back_2"));
		Files.delete(Paths.get(location+"mapping_back_type_1"));
		Files.delete(Paths.get(location+"mapping_back_type_2"));
		System.out.println("Generating header");
		this.header = HeaderFactory.createHeader(spec);
		this.populateHeaderStructure("http://wdaqua.eu/hdtCat/");
	}

	public void diff(HDT hdt1, HDT hdt2, ProgressListener listener) throws IOException {
		ModifiableBitmap bitmap = BitmapFactory.createRWBitmap(hdt1.getTriples().getNumberOfElements());
		BitmapTriplesIteratorDiff iterator = new BitmapTriplesIteratorDiff(hdt1, hdt2, bitmap);
		iterator.fillBitmap();
		diffBit(getHDTFileName(), hdt1, bitmap, listener);
	}

	public void diffBit(String location, HDT hdt, Bitmap deleteBitmap, ProgressListener listener) throws IOException {
		IntermediateListener il = new IntermediateListener(listener);
		log.debug("Generating Dictionary...");
		il.notifyProgress(0, "Generating Dictionary...");
		IteratorTripleID hdtIterator = hdt.getTriples().searchAll();
		DictionaryEntriesDiff iter = DictionaryEntriesDiff.createForType(hdt.getDictionary(), hdt, deleteBitmap, hdtIterator);

		iter.loadBitmaps();

		Map<String, ModifiableBitmap> bitmaps = iter.getBitmaps();

		DictionaryDiff diff = DictionaryFactory.createDictionaryDiff(hdt.getDictionary(), location);

		diff.diff(hdt.getDictionary(), bitmaps, listener);

		//map the generated dictionary
		ControlInfo ci2 = new ControlInformation();

		try (CountInputStream fis = new CountInputStream(new BufferedInputStream(new FileInputStream(location + "dictionary")))) {
			fis.mark(1024);
			ci2.load(fis);
			fis.reset();
			DictionaryPrivate dictionary = DictionaryFactory.createDictionary(ci2);
			dictionary.mapFromFile(fis, new File(location + "dictionary"), null);
			this.dictionary = dictionary;
		}
		log.debug("Generating Triples...");
		il.notifyProgress(40, "Generating Triples...");
		// map the triples based on the new dictionary
		BitmapTriplesIteratorMapDiff mapIter = new BitmapTriplesIteratorMapDiff(hdt, deleteBitmap, diff, iter.getCount() + 1);
		BitmapTriples triples = new BitmapTriples(spec);
		triples.load(mapIter, listener);
		this.triples = triples;

		log.debug("Clear data...");
		il.notifyProgress(80, "Clear data...");
		if(!(hdt.getDictionary() instanceof FourSectionDictionary)) {
			int count = 0;
			for (Map.Entry<String, DictionarySection> next : dictionary.getAllObjects().entrySet()) {
				String subPrefix = "sub" + count;
				if(next.getKey().equals("NO_DATATYPE"))
					subPrefix = next.getKey();
				Files.delete(Paths.get(location + subPrefix));
				Files.delete(Paths.get(location + subPrefix + "Types"));
				count++;
			}
		}

		try {
			Files.delete(Paths.get(location+"predicate"));
			Files.delete(Paths.get(location+"predicate"+"Types"));
			Files.delete(Paths.get(location+"subject"));
			Files.delete(Paths.get(location+"subject"+"Types"));
			Files.delete(Paths.get(location+"object"));
			Files.delete(Paths.get(location+"object"+"Types"));
			Files.delete(Paths.get(location+"shared"));
			Files.delete(Paths.get(location+"shared"+"Types"));
			Files.delete(Paths.get(location+"back"+"Types"));
			Files.delete(Paths.get(location+"back"));
			Files.deleteIfExists(Paths.get(location+"P"));
			Files.deleteIfExists(Paths.get(location+"S"));
			Files.deleteIfExists(Paths.get(location+"O"));
			Files.deleteIfExists(Paths.get(location+"SH_S"));
			Files.deleteIfExists(Paths.get(location+"SH_O"));
		} catch (IOException e) {
			e.printStackTrace();
		}

		log.debug("Set header...");
		il.notifyProgress(90, "Set header...");
		this.header = HeaderFactory.createHeader(spec);

		this.populateHeaderStructure("http://wdaqua.eu/hdtDiff/");
		log.debug("Diff completed.");
		il.notifyProgress(100, "Diff completed...");
	}
}
