package org.rdfhdt.hdt.iterator.utils;

import org.rdfhdt.hdt.triples.TripleString;
import org.rdfhdt.hdt.util.string.ByteStringUtil;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.function.Consumer;
import java.util.function.ToIntFunction;

/**
 * Iterator to split an iterator stream into multiple files, the iterator return {@link #hasNext()} == true once the
 * first file is returned, then the {@link #hasNewFile()} should be called to check if another file can be created and
 * re-allow {@link #hasNext()} to return true
 * @author Antoine Willerval
 */
public class FileTripleIterator extends FileChunkIterator<TripleString> {
	private static long estimateSize(TripleString tripleString) {
		try {
			return tripleString.asNtriple().toString().getBytes(ByteStringUtil.STRING_ENCODING).length;
		} catch (IOException e) {
			throw new RuntimeException("Can't estimate the size of the triple " + tripleString, e);
		}
	}

	/**
	 * create a file iterator from a stream and a max size
	 *
	 * @param it                 the iterator
	 * @param maxSize            the maximum size of each file, this size is estimated, so files can be bigger.
	 */
	public FileTripleIterator(Iterator<TripleString> it, long maxSize) {
		super(it, maxSize, FileTripleIterator::estimateSize);
	}

}