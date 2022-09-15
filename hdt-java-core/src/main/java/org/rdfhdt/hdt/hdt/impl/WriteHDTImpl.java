package org.rdfhdt.hdt.hdt.impl;

import org.rdfhdt.hdt.dictionary.impl.WriteFourSectionDictionary;
import org.rdfhdt.hdt.exceptions.NotImplementedException;
import org.rdfhdt.hdt.header.HeaderFactory;
import org.rdfhdt.hdt.header.HeaderPrivate;
import org.rdfhdt.hdt.listener.ProgressListener;
import org.rdfhdt.hdt.options.HDTOptions;
import org.rdfhdt.hdt.triples.IteratorTripleString;
import org.rdfhdt.hdt.triples.impl.WriteBitmapTriples;
import org.rdfhdt.hdt.util.io.CloseSuppressPath;
import org.rdfhdt.hdt.util.io.IOUtil;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * HDT implementation to write on disk the components
 *
 * @author Antoine Willerval
 */
public class WriteHDTImpl extends HDTBase<HeaderPrivate, WriteFourSectionDictionary, WriteBitmapTriples> {
	private String baseURI;
	private final CloseSuppressPath workingLocation;
	private boolean isClosed;

	public WriteHDTImpl(HDTOptions spec, CloseSuppressPath workingLocation, int bufferSize) throws IOException {
		super(spec);
		this.workingLocation = workingLocation;
		workingLocation.mkdirs();

		dictionary = new WriteFourSectionDictionary(this.spec, workingLocation.resolve("section"), bufferSize);
		// we need to have the bitmaps in memory, so we can't bypass the implementation
		triples = new WriteBitmapTriples(this.spec, workingLocation.resolve("tripleBitmap"), bufferSize);
		// small, can use default implementation
		header = HeaderFactory.createHeader(this.spec);
	}

	@Override
	public void setBaseUri(String baseURI) {
		this.baseURI = baseURI;
	}

	@Override
	public void loadFromHDT(InputStream input, ProgressListener listener) {
		throw new NotImplementedException();
	}

	@Override
	public void loadFromHDT(String fileName, ProgressListener listener) {
		throw new NotImplementedException();
	}

	@Override
	public void mapFromHDT(File f, long offset, ProgressListener listener) {
		throw new NotImplementedException();
	}

	@Override
	public void loadOrCreateIndex(ProgressListener listener) {
		throw new NotImplementedException();
	}

	@Override
	public void saveToHDT(String fileName, ProgressListener listener) throws IOException {
		try (OutputStream out = new BufferedOutputStream(Files.newOutputStream(Path.of(fileName)))) {
			saveToHDT(out, listener);
		}
	}

	@Override
	public long size() {
		if (isClosed)
			return 0;

		return getDictionary().size() + getTriples().size();
	}

	@Override
	public String getBaseURI() {
		return baseURI;
	}

	@Override
	public boolean isClosed() {
		return isClosed;
	}

	@Override
	public void close() throws IOException {
		if (isClosed()) {
			return;
		}
		isClosed = true;
		IOUtil.closeAll(
				dictionary,
				triples,
				workingLocation
		);
	}

	@Override
	public IteratorTripleString search(CharSequence subject, CharSequence predicate, CharSequence object) {
		throw new NotImplementedException();
	}
}
