package org.rdfhdt.hdt.dictionary.impl;

import org.rdfhdt.hdt.compact.integer.VByte;
import org.rdfhdt.hdt.dictionary.TempDictionary;
import org.rdfhdt.hdt.dictionary.TempDictionarySection;
import org.rdfhdt.hdt.dictionary.impl.section.OneReadDictionarySection;
import org.rdfhdt.hdt.enums.TripleComponentRole;
import org.rdfhdt.hdt.exceptions.NotImplementedException;
import org.rdfhdt.hdt.hdt.impl.diskimport.CompressionResult;
import org.rdfhdt.hdt.iterator.utils.MapIterator;
import org.rdfhdt.hdt.iterator.utils.PipedCopyIterator;
import org.rdfhdt.hdt.triples.IndexedNode;
import org.rdfhdt.hdt.triples.TempTriples;
import org.rdfhdt.hdt.util.io.IOUtil;
import org.rdfhdt.hdt.util.io.compress.CompressUtil;
import org.rdfhdt.hdt.util.string.ByteStringUtil;
import org.rdfhdt.hdt.util.string.CharSequenceComparator;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Comparator;
import java.util.Iterator;

/**
 * Version of temp dictionary create the four sections from the SPO compressed sections result, should be loaded in a
 * async way with {@link org.rdfhdt.hdt.dictionary.DictionaryPrivate#loadAsync(org.rdfhdt.hdt.dictionary.TempDictionary, org.rdfhdt.hdt.listener.ProgressListener)}
 * @author Antoine Willerval
 */
public class CompressFourSectionDictionary implements TempDictionary {
	private final TempDictionarySection subject;
	private final TempDictionarySection predicate;
	private final TempDictionarySection object;
	private final TempDictionarySection shared;

	private static void sendPiped(IndexedNode value, PipedCopyIterator<IndexedNode> pipe) {
		pipe.addElement(new IndexedNode(value.getNode(), value.getIndex()));
	}

	public CompressFourSectionDictionary(CompressionResult compressionResult, NodeConsumer nodeConsumer) {
		// send duplicate to the consumer while reading the nodes
		Iterator<? extends IndexedNode> sortedSubject =
				CompressUtil.asNoDupeCharSequenceIterator(
						compressionResult.getSubjects(),
						(originalIndex, duplicatedIndex) -> nodeConsumer.onSubject(duplicatedIndex, CompressUtil.asDuplicated(originalIndex))
				);
		Iterator<? extends IndexedNode> sortedPredicate =
				CompressUtil.asNoDupeCharSequenceIterator(
						compressionResult.getPredicates(),
						(originalIndex, duplicatedIndex) -> nodeConsumer.onPredicate(duplicatedIndex, CompressUtil.asDuplicated(originalIndex))
				);
		Iterator<? extends IndexedNode> sortedObject =
				CompressUtil.asNoDupeCharSequenceIterator(
						compressionResult.getObjects(),
						(originalIndex, duplicatedIndex) -> nodeConsumer.onObject(duplicatedIndex, CompressUtil.asDuplicated(originalIndex))
				);
		long subjects = compressionResult.getSubjectsCount();
		long predicates = compressionResult.getPredicatesCount();
		long objects = compressionResult.getObjectsCount();
		long shareds = compressionResult.getSharedCount();

		// iterator to pipe to the s p o sh
		PipedCopyIterator<IndexedNode> subject = new PipedCopyIterator<>(new IndexedNodeParser());
		PipedCopyIterator<IndexedNode> object = new PipedCopyIterator<>(new IndexedNodeParser());
		PipedCopyIterator<SharedNode> shared = new PipedCopyIterator<>(new SharedNodeParser());
		Comparator<CharSequence> comparator = CharSequenceComparator.getInstance();
		Thread readingThread = new Thread(() -> {
			sharedLoop:
			while (sortedObject.hasNext() && sortedSubject.hasNext()) {
				// last was a shared node
				IndexedNode newSubject = sortedSubject.next();
				IndexedNode newObject = sortedObject.next();
				int comp = comparator.compare(newSubject.getNode(), newObject.getNode());
				while (comp != 0) {
					if (comp < 0) {
						sendPiped(newSubject, subject);
						if (!sortedSubject.hasNext()) {
							// no more subjects, send the current object and break the shared loop
							sendPiped(newObject, object);
							break sharedLoop;
						}
						newSubject = sortedSubject.next();
					} else {
						sendPiped(newObject, object);
						if (!sortedObject.hasNext()) {
							// no more objects, send the current subject and break the shared loop
							sendPiped(newSubject, subject);
							break sharedLoop;
						}
						newObject = sortedObject.next();
					}
					comp = comparator.compare(newSubject.getNode(), newObject.getNode());
				}
				// shared element
				shared.addElement(new SharedNode(newSubject.getIndex(), newObject.getIndex(), newSubject.getNode()));
			}
			// at least one iterator is empty, closing the shared pipe
			shared.closePipe();
			// do we have subjects?
			while (sortedSubject.hasNext()) {
				sendPiped(sortedSubject.next(), subject);
			}
			subject.closePipe();
			// do we have objects?
			while (sortedObject.hasNext()) {
				sendPiped(sortedObject.next(), object);
			}
			object.closePipe();
		}, "CFSDPipeBuilder");
		readingThread.start();

		// send to the consumer the element while parsing them
		this.subject = new OneReadDictionarySection(subject.mapWithId((node, index) -> {
			nodeConsumer.onSubject(node.getIndex(), index + 1);
			return node.getNode();
		}), subjects);
		this.predicate = new OneReadDictionarySection(new MapIterator<>(sortedPredicate, (node, index) -> {
			nodeConsumer.onPredicate(node.getIndex(), index + 1);
			// force duplication because it's not made in a pipe like with the others
			return node.getNode().toString();
		}), predicates);
		this.object = new OneReadDictionarySection(object.mapWithId((node, index) -> {
			nodeConsumer.onObject(node.getIndex(), index + 1);
			return node.getNode();
		}), objects);
		this.shared = new OneReadDictionarySection(shared.mapWithId((node, index) -> {
			long sharedIndex = CompressUtil.asShared(index + 1);
			nodeConsumer.onSubject(node.indexSubject, sharedIndex);
			nodeConsumer.onObject(node.indexObject, sharedIndex);
			return node.node;
		}), shareds);
	}

	@Override
	public TempDictionarySection getSubjects() {
		return subject;
	}

	@Override
	public TempDictionarySection getPredicates() {
		return predicate;
	}

	@Override
	public TempDictionarySection getObjects() {
		return object;
	}

	@Override
	public TempDictionarySection getShared() {
		return shared;
	}

	@Override
	public void startProcessing() {
	}

	@Override
	public void endProcessing() {
	}

	@Override
	public long insert(CharSequence str, TripleComponentRole position) {
		throw new NotImplementedException();
	}

	@Override
	public void reorganize() {
		// already organized
	}

	@Override
	public void reorganize(TempTriples triples) {
		// already organized
	}

	@Override
	public boolean isOrganized() {
		return true;
	}

	@Override
	public void clear() {
	}

	@Override
	public long stringToId(CharSequence subject, TripleComponentRole role) {
		throw new NotImplementedException();
	}

	@Override
	public void close() throws IOException {
	}

	public interface NodeConsumer {
		void onSubject(long preMapId, long newMapId);
		void onPredicate(long preMapId, long newMapId);
		void onObject(long preMapId, long newMapId);
	}

	private static class SharedNodeParser implements PipedCopyIterator.Parser<SharedNode> {
		@Override
		public void write(SharedNode sharedNode, OutputStream out) throws IOException {
			VByte.encode(out, sharedNode.indexSubject);
			VByte.encode(out, sharedNode.indexObject);
			byte[] bytes = sharedNode.node.toString().getBytes(ByteStringUtil.STRING_ENCODING);
			VByte.encode(out, bytes.length);
			out.write(bytes);
		}

		@Override
		public SharedNode read(InputStream in) throws IOException {
			long indexSubject = VByte.decode(in);
			long indexObject = VByte.decode(in);
			int size = (int) VByte.decode(in);
			byte[] bytes = IOUtil.readBuffer(in, size, null);
			return new SharedNode(indexSubject, indexObject, new String(bytes, ByteStringUtil.STRING_ENCODING));
		}
	}
	private static class IndexedNodeParser implements PipedCopyIterator.Parser<IndexedNode> {
		@Override
		public void write(IndexedNode indexedNode, OutputStream out) throws IOException {
			VByte.encode(out, indexedNode.getIndex());
			byte[] bytes = indexedNode.getNode().toString().getBytes(ByteStringUtil.STRING_ENCODING);
			VByte.encode(out, bytes.length);
			out.write(bytes);
		}

		@Override
		public IndexedNode read(InputStream in) throws IOException {
			long sid = VByte.decode(in);
			int size = (int) VByte.decode(in);
			byte[] bytes = IOUtil.readBuffer(in, size, null);
			return new IndexedNode(new String(bytes, ByteStringUtil.STRING_ENCODING), sid);
		}
	}
	private static class SharedNode {
		long indexSubject;
		long indexObject;
		CharSequence node;

		public SharedNode(long indexSubject, long indexObject, CharSequence node) {
			this.indexSubject = indexSubject;
			this.indexObject = indexObject;
			this.node = node;
		}
	}

}
