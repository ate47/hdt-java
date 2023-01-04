package org.rdfhdt.hdt.rdf.parsers;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.rdfhdt.hdt.enums.RDFNotation;
import org.rdfhdt.hdt.exceptions.ParserException;
import org.rdfhdt.hdt.options.HDTOptions;
import org.rdfhdt.hdt.rdf.RDFParserCallback;
import org.rdfhdt.hdt.rdf.RDFParserFactory;
import org.rdfhdt.hdt.util.io.IOUtil;
import org.rdfhdt.hdt.util.io.NonCloseInputStream;

/**
 * Parses a tar file (optionally .tgz or .tar.gz or .tar.bz2) directly, processing each file that contains rdf separately.
 * 
 * It uses RDFNotation.guess() to guess the format of each specific file. If not recognised, each file of the tar is ignored.
 *  
 *  
 * @author 
 *
 */

public class RDFParserZip implements RDFParserCallback {

	private final HDTOptions spec;

	public RDFParserZip(HDTOptions spec) {
		this.spec = spec;
	}

	public RDFParserZip() {
		this(HDTOptions.EMPTY);
	}

	/* (non-Javadoc)
	 * @see hdt.rdf.RDFParserCallback#doParse(java.lang.String, java.lang.String, hdt.enums.RDFNotation, hdt.rdf.RDFParserCallback.Callback)
	 */
	@Override
	public void doParse(String fileName, String baseUri, RDFNotation notation, boolean keepBNode, RDFCallback callback) throws ParserException {
		try {
			InputStream input = IOUtil.getFileInputStream(fileName);
			this.doParse(input, baseUri, notation, keepBNode, callback);
			input.close();
		} catch (Exception e) {
			e.printStackTrace();
			throw new ParserException();
		} 
	}

	@Override
	public void doParse(InputStream input, String baseUri, RDFNotation notation, boolean keepBNode, RDFCallback callback) throws ParserException {
		try {
			ZipInputStream zin = new ZipInputStream(input);
			
			// Don't allow the inner parser to close the InputStream to continue reading entries.
			InputStream nonCloseIn = new NonCloseInputStream(zin);
			
	        for (ZipEntry zipEntry;(zipEntry = zin.getNextEntry()) != null; )
	        {
	        	if(!zipEntry.isDirectory()) {
					try {
						RDFNotation guessnot = RDFNotation.guess(zipEntry.getName());
						System.out.println("Parse from zip: "+zipEntry.getName()+" as "+guessnot);
						RDFParserCallback parser = RDFParserFactory.getParserCallback(guessnot, spec);

						parser.doParse(nonCloseIn, baseUri, guessnot, keepBNode, callback);
					} catch (IllegalArgumentException | ParserException e1) {
						e1.printStackTrace();
					}
	        	}
	        }
	        // Don't close passed stream.

		} catch (Exception e) {
			e.printStackTrace();
			throw new ParserException(e);
		}
	}

}
