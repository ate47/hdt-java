package org.rdfhdt.hdt.dictionary.impl;

import org.rdfhdt.hdt.compact.integer.VByte;
import org.rdfhdt.hdt.dictionary.TempDictionary;
import org.rdfhdt.hdt.dictionary.TempDictionarySection;
import org.rdfhdt.hdt.dictionary.impl.section.OneReadDictionarySection;
import org.rdfhdt.hdt.enums.TripleComponentRole;
import org.rdfhdt.hdt.exceptions.NotImplementedException;
import org.rdfhdt.hdt.hdt.impl.diskimport.CompressionResult;
import org.rdfhdt.hdt.iterator.utils.MapIterator;
import org.rdfhdt.hdt.iterator.utils.NotificationExceptionIterator;
import org.rdfhdt.hdt.iterator.utils.PipedCopyIterator;
import org.rdfhdt.hdt.listener.ProgressListener;
import org.rdfhdt.hdt.triples.IndexedNode;
import org.rdfhdt.hdt.triples.TempTriples;
import org.rdfhdt.hdt.util.concurrent.ExceptionThread;
import org.rdfhdt.hdt.util.io.IOUtil;
import org.rdfhdt.hdt.util.io.compress.CompressUtil;
import org.rdfhdt.hdt.util.string.ByteStringUtil;
import org.rdfhdt.hdt.util.string.CharSequenceComparator;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Comparator;

/**
 * Version of temp dictionary create the four sections from the SPO compressed sections result, should be loaded in a
 * async way with {@link org.rdfhdt.hdt.dictionary.DictionaryPrivate#loadAsync(org.rdfhdt.hdt.dictionary.TempDictionary, org.rdfhdt.hdt.listener.ProgressListener)}
 * @author Antoine Willerval
 */
public class CompressFourSectionDictionary implements TempDictionary {
	private final ExceptionThread cfsdThread;
	private final TempDictionarySection subject;
	private final TempDictionarySection predicate;
	private final TempDictionarySection object;
	private final TempDictionarySection shared;

	private static void sendPiped(IndexedNode node, long index, PipedCopyIterator<CharSequence> pipe, CompressUtil.DuplicatedIterator it, NodeConsumerMethod method) {
		it.setLastHeader(index);
		method.consume(node.getIndex(), index);
		pipe.addElement(node.getNode());
	}

	public CompressFourSectionDictionary(CompressionResult compressionResult, NodeConsumer nodeConsumer, ProgressListener listener) {
		long splits = Math.max(20, compressionResult.getTripleCount() / 10_000);
		// send duplicate to the consumer while reading the nodes
		CompressUtil.DuplicatedIterator sortedSubject =
				CompressUtil.asNoDupeCharSequenceIterator(
						new NotificationExceptionIterator<>(
								compressionResult.getSubjects(),
								compressionResult.getTripleCount(),
								splits,
								"Subject section filling",
								listener
						),
						(originalIndex, duplicatedIndex, lastHeader) -> nodeConsumer.onSubject(duplicatedIndex, lastHeader)
				);
		CompressUtil.DuplicatedIterator sortedPredicate =
				CompressUtil.asNoDupeCharSequenceIterator(
						new NotificationExceptionIterator<>(
								compressionResult.getPredicates(),
								compressionResult.getTripleCount(),
								splits,
								"Predicate section filling",
								listener
						),
						(originalIndex, duplicatedIndex, lastHeader) -> nodeConsumer.onPredicate(duplicatedIndex, lastHeader)
				);
		CompressUtil.DuplicatedIterator sortedObject =
				CompressUtil.asNoDupeCharSequenceIterator(
						new NotificationExceptionIterator<>(
								compressionResult.getObjects(),
								compressionResult.getTripleCount(),
								splits,
								"Object section filling",
								listener
						),
						(originalIndex, duplicatedIndex, lastHeader) -> nodeConsumer.onObject(duplicatedIndex, lastHeader)
				);
		long subjects = compressionResult.getSubjectsCount();
		long predicates = compressionResult.getPredicatesCount();
		long objects = compressionResult.getObjectsCount();
		long shareds = compressionResult.getSharedCount();

		// iterator to pipe to the s p o sh
		PipedCopyIterator<CharSequence> subject = new PipedCopyIterator<>(new StringParser());
		PipedCopyIterator<CharSequence> object = new PipedCopyIterator<>(new StringParser());
		PipedCopyIterator<CharSequence> shared = new PipedCopyIterator<>(new StringParser());
		Comparator<CharSequence> comparator = CharSequenceComparator.getInstance();
		cfsdThread = new ExceptionThread(() -> {
			long sharedId = 1;
			long subjectId = 1;
			long objectId = 1;
			sharedLoop:
			while (sortedObject.hasNext() && sortedSubject.hasNext()) {
				// last was a shared node
				IndexedNode newSubject = sortedSubject.next();
				IndexedNode newObject = sortedObject.next();
				int comp = comparator.compare(newSubject.getNode(), newObject.getNode());
				while (comp != 0) {
					if (comp < 0) {
						sendPiped(newSubject, CompressUtil.getHeaderId(subjectId++), subject, sortedSubject, nodeConsumer::onSubject);
						if (!sortedSubject.hasNext()) {
							// no more subjects, send the current object and break the shared loop
							sendPiped(newObject, CompressUtil.getHeaderId(objectId++), object, sortedObject, nodeConsumer::onObject);
							break sharedLoop;
						}
						newSubject = sortedSubject.next();
					} else {
						sendPiped(newObject, CompressUtil.getHeaderId(objectId++), object, sortedObject, nodeConsumer::onObject);
						if (!sortedObject.hasNext()) {
							// no more objects, send the current subject and break the shared loop
							sendPiped(newSubject, CompressUtil.getHeaderId(subjectId++), subject, sortedSubject, nodeConsumer::onSubject);
							break sharedLoop;
						}
						newObject = sortedObject.next();
					}
					comp = comparator.compare(newSubject.getNode(), newObject.getNode());
				}
				// shared element
				long shid = CompressUtil.asShared(sharedId++);
				sortedSubject.setLastHeader(shid);
				sortedObject.setLastHeader(shid);
				nodeConsumer.onSubject(newSubject.getIndex(), shid);
				nodeConsumer.onObject(newObject.getIndex(), shid);
				shared.addElement(newSubject.getNode());
			}
			// at least one iterator is empty, closing the shared pipe
			shared.closePipe();
			// do we have subjects?
			while (sortedSubject.hasNext()) {
				sendPiped(sortedSubject.next(), CompressUtil.getHeaderId(subjectId++), subject, sortedSubject, nodeConsumer::onSubject);
			}
			subject.closePipe();
			// do we have objects?
			while (sortedObject.hasNext()) {
				sendPiped(sortedObject.next(), CompressUtil.getHeaderId(objectId++), object, sortedObject, nodeConsumer::onObject);
			}
			object.closePipe();
		}, "CFSDPipeBuilder").startAll();

		// send to the consumer the element while parsing them
		this.subject = new OneReadDictionarySection(subject, subjects);
		this.predicate = new OneReadDictionarySection(new MapIterator<>(sortedPredicate, (node, index) -> {
			long header = CompressUtil.getHeaderId(index + 1);
			sortedPredicate.setLastHeader(header);
			nodeConsumer.onPredicate(node.getIndex(), header);
			// force duplication because it's not made in a pipe like with the others
			return node.getNode().toString();
		}), predicates);
		this.object = new OneReadDictionarySection(object, objects);
		this.shared = new OneReadDictionarySection(shared, shareds);
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
		try {
			cfsdThread.interrupt();
			cfsdThread.joinAndCrashIfRequired();
		} catch (InterruptedException e) {
			// normal
		}
	}

	public interface NodeConsumer {
		void onSubject(long preMapId, long newMapId);
		void onPredicate(long preMapId, long newMapId);
		void onObject(long preMapId, long newMapId);
	}

	private static class StringParser implements PipedCopyIterator.Parser<CharSequence> {
		@Override
		public void write(CharSequence node, OutputStream out) throws IOException {
			byte[] bytes = node.toString().getBytes(ByteStringUtil.STRING_ENCODING);
			VByte.encode(out, bytes.length);
			out.write(bytes);
		}

		@Override
		public String read(InputStream in) throws IOException {
			int size = (int) VByte.decode(in);
			byte[] bytes = IOUtil.readBuffer(in, size, null);
			return new String(bytes, ByteStringUtil.STRING_ENCODING);
		}
	}
	private interface NodeConsumerMethod {
		void consume(long id, long header);
	}
}
