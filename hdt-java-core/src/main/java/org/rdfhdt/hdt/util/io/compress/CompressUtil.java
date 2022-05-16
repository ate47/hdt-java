package org.rdfhdt.hdt.util.io.compress;

import org.rdfhdt.hdt.iterator.utils.ExceptionIterator;
import org.rdfhdt.hdt.triples.IndexedNode;
import org.rdfhdt.hdt.util.string.CharSequenceComparator;
import org.rdfhdt.hdt.util.string.ReplazableString;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * Utility class to manipulate compressed node
 *
 * @author Antoine Willerval
 */
public class CompressUtil {
	/**
	 * the mask for shared computed compressed node
	 */
	public static final long SHARED_MASK = 1L;
	/**
	 * mask for duplicated node
	 */
	public static final long DUPLICATE_MASK = 1L << 1;
	/**
	 * shift after the SHARED/DUPLICATES
	 */
	public static final int INDEX_SHIFT = 2;

	/**
	 * write a sorted list of indexed node
	 *
	 * @param strings the nodes to write
	 * @param output  the output
	 * @throws IOException writing exception
	 */
	public static void writeCompressedSection(List<IndexedNode> strings, OutputStream output) throws IOException {
		writeCompressedSection(ExceptionIterator.of(strings.iterator()), strings.size(), output);
	}

	/**
	 * write a sorted iterator of indexed node
	 *
	 * @param it     iterator to write
	 * @param size   size of the iterator
	 * @param output the output where to write
	 * @throws IOException writing exception
	 */
	public static void writeCompressedSection(ExceptionIterator<IndexedNode, IOException> it, long size, OutputStream output) throws IOException {
		CompressNodeWriter writer = new CompressNodeWriter(output, size);
		it.forEachRemaining(writer::appendNode);
		writer.writeCRC();
	}

	/**
	 * merge two stream together into an output stream
	 *
	 * @param stream1 input stream 1
	 * @param stream2 input stream 2
	 * @param output  output stream
	 * @throws IOException read/writing exception
	 */
	public static void mergeCompressedSection(InputStream stream1, InputStream stream2, OutputStream output) throws IOException {
		CompressNodeReader in1r = new CompressNodeReader(stream1);
		CompressNodeReader in2r = new CompressNodeReader(stream2);

		long size1 = in1r.getSize();
		long size2 = in2r.getSize();

		// merge the section
		writeCompressedSection(new CompressNodeMergeIterator(in1r, in2r), size1 + size2, output);
		// check we have completed the 2 readers
		in1r.checkComplete();
		in2r.checkComplete();
	}

	/**
	 * compute the shared-computed id from a shared-computable id
	 *
	 * @param id          the shared-computable id
	 * @param sharedCount the count of shared elements
	 * @return the shared-computed element
	 */
	public static long computeSharedNode(long id, long sharedCount) {
		if ((id & SHARED_MASK) != 0) {
			// shared element
			return CompressUtil.getId(id);
		}
		// not shared
		return CompressUtil.getId(id) + sharedCount;
	}

	/**
	 * convert this id to a shared-computable element
	 *
	 * @param id the id
	 * @return shared-computable element
	 */
	public static long asShared(long id) {
		return getHeaderId(id) | SHARED_MASK;
	}

	/**
	 * convert this id to a duplicated computable element
	 *
	 * @param id the id
	 * @return duplicated computable element
	 */
	public static long asDuplicated(long id) {
		return getHeaderId(id) | DUPLICATE_MASK;
	}

	/**
	 * test if this id is a duplicated element
	 *
	 * @param id the id
	 * @return true if id is a duplicated element, false otherwise
	 */
	public static boolean isDuplicated(long id) {
		return (id & DUPLICATE_MASK) != 0;
	}

	/**
	 * get the id from a header id
	 * @param headerId the header id
	 * @return the id
	 */
	public static long getId(long headerId) {
		return headerId >>> INDEX_SHIFT;
	}

	/**
	 * get a header id from an id
	 * @param id the id
	 * @return the header id
	 */
	public static long getHeaderId(long id) {
		return id << INDEX_SHIFT;
	}

	/**
	 * @return a char sequence base iterator view of this iterator
	 */
	public static Iterator<IndexedNode> asNoDupeCharSequenceIterator(ExceptionIterator<IndexedNode, ?> nodes, DuplicatedNodeConsumer duplicatedNodeConsumer) {
		return new CharSeqIteratorView(nodes.asIterator(), duplicatedNodeConsumer);
	}

	@FunctionalInterface
	public interface DuplicatedNodeConsumer {
		void onDuplicated(long originalIndex, long duplicatedIndex);
	}

	private static class CharSeqIteratorView implements Iterator<IndexedNode> {
		private final Iterator<IndexedNode> it;
		private final ReplazableString prev = new ReplazableString();
		private IndexedNode next;
		private long id;
		private final DuplicatedNodeConsumer duplicatedNodeConsumer;

		CharSeqIteratorView(Iterator<IndexedNode> it, DuplicatedNodeConsumer duplicatedNodeConsumer) {
			this.it = it;
			this.duplicatedNodeConsumer = Objects.requireNonNullElseGet(duplicatedNodeConsumer, () -> (i, j) -> {
			});
		}

		@Override
		public boolean hasNext() {
			if (next != null) {
				return true;
			}
			while (it.hasNext()) {
				IndexedNode node = it.next();
				CharSequence next = node.getNode();
				if (CharSequenceComparator.getInstance().compare(prev, next) == 0) {
					// same as previous, ignore
					assert this.id != node.getIndex() : "same index and prevIndex";
					duplicatedNodeConsumer.onDuplicated(this.id, node.getIndex());
					continue;
				}
				this.next = node;
				prev.replace(next);
				this.id = node.getIndex();
				return true;
			}
			return false;
		}

		@Override
		public IndexedNode next() {
			IndexedNode old = next;
			next = null;
			return old;
		}
	}

	private CompressUtil() {
	}
}
