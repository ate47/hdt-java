package org.rdfhdt.hdt.dictionary.impl;

import org.rdfhdt.hdt.dictionary.TempDictionary;
import org.rdfhdt.hdt.dictionary.TempDictionarySection;
import org.rdfhdt.hdt.dictionary.impl.section.OneReadDictionarySection;
import org.rdfhdt.hdt.enums.TripleComponentRole;
import org.rdfhdt.hdt.exceptions.NotImplementedException;
import org.rdfhdt.hdt.hdt.impl.diskimport.CompressionResult;
import org.rdfhdt.hdt.iterator.utils.ExceptionIterator;
import org.rdfhdt.hdt.iterator.utils.MapIterator;
import org.rdfhdt.hdt.iterator.utils.NotificationExceptionIterator;
import org.rdfhdt.hdt.iterator.utils.PipedCopyIterator;
import org.rdfhdt.hdt.listener.ProgressListener;
import org.rdfhdt.hdt.triples.IndexedNode;
import org.rdfhdt.hdt.triples.TempTriples;
import org.rdfhdt.hdt.util.concurrent.ExceptionThread;
import org.rdfhdt.hdt.util.io.compress.CompressUtil;
import org.rdfhdt.hdt.util.string.CharSequenceComparator;
import org.rdfhdt.hdt.util.string.CompactString;
import org.rdfhdt.hdt.utils.DebugOrderNodeIterator;

import java.io.IOException;
import java.util.Comparator;
import java.util.function.Consumer;

/**
 * Version of temp dictionary create the four sections from the SPO compressed sections result, should be loaded in a
 * async way with {@link org.rdfhdt.hdt.dictionary.DictionaryPrivate#loadAsync(org.rdfhdt.hdt.dictionary.TempDictionary, org.rdfhdt.hdt.listener.ProgressListener)}
 *
 * @author Antoine Willerval
 */
public class CompressFourSectionDictionary implements TempDictionary {
	private final ExceptionThread cfsdThread;
	private final TempDictionarySection subject;
	private final TempDictionarySection predicate;
	private final TempDictionarySection object;
	private final TempDictionarySection shared;

	private static void sendPiped(IndexedNode node, long index, PipedCopyIterator<CharSequence> pipe, NodeConsumerMethod method) {
		method.consume(node.getIndex(), index);
		pipe.addElement(new CompactString(node.getNode()));
	}

	public CompressFourSectionDictionary(CompressionResult compressionResult, NodeConsumer nodeConsumer, ProgressListener listener, boolean debugOrder) {
		long splits = Math.max(20, compressionResult.getTripleCount() / 10_000);
		Consumer<IndexedNode> debugOrderCheckerS = DebugOrderNodeIterator.of(debugOrder, "Subject");
		Consumer<IndexedNode> debugOrderCheckerO = DebugOrderNodeIterator.of(debugOrder, "Object");
		// send duplicate to the consumer while reading the nodes
		ExceptionIterator<? extends IndexedNode, IOException> sortedSubject =
				new NotificationExceptionIterator<>(
						compressionResult.getSubjects(),
						compressionResult.getTripleCount(),
						splits,
						"Subject section filling",
						listener
				);
		ExceptionIterator<? extends IndexedNode, IOException> sortedPredicate =
				new NotificationExceptionIterator<>(
						compressionResult.getPredicates(),
						compressionResult.getTripleCount(),
						splits,
						"Predicate section filling",
						listener
				);
		ExceptionIterator<? extends IndexedNode, IOException> sortedObject =
				new NotificationExceptionIterator<>(
						compressionResult.getObjects(),
						compressionResult.getTripleCount(),
						splits,
						"Object section filling",
						listener
				);
		long subjects = compressionResult.getSubjectsCount();
		long predicates = compressionResult.getPredicatesCount();
		long objects = compressionResult.getObjectsCount();
		long shareds = compressionResult.getSharedCount();

		// iterator to pipe to the s p o sh
		PipedCopyIterator<CharSequence> subject = new PipedCopyIterator<>();
		PipedCopyIterator<CharSequence> object = new PipedCopyIterator<>();
		PipedCopyIterator<CharSequence> shared = new PipedCopyIterator<>();
		Comparator<CharSequence> comparator = CharSequenceComparator.getInstance();
		cfsdThread = new ExceptionThread(() -> {
			try {
				long sharedId = 1;
				long subjectId = 1;
				long objectId = 1;
				sharedLoop:
				while (sortedObject.hasNext() && sortedSubject.hasNext()) {
					// last was a shared node
					IndexedNode newSubject = sortedSubject.next();
					IndexedNode newObject = sortedObject.next();
					debugOrderCheckerS.accept(newSubject);
					debugOrderCheckerO.accept(newObject);
					int comp = comparator.compare(newSubject.getNode(), newObject.getNode());
					while (comp != 0) {
						if (comp < 0) {
							sendPiped(newSubject, CompressUtil.getHeaderId(subjectId++), subject, nodeConsumer::onSubject);
							if (!sortedSubject.hasNext()) {
								// no more subjects, send the current object and break the shared loop
								sendPiped(newObject, CompressUtil.getHeaderId(objectId++), object, nodeConsumer::onObject);
								break sharedLoop;
							}
							newSubject = sortedSubject.next();
							debugOrderCheckerS.accept(newSubject);
						} else {
							sendPiped(newObject, CompressUtil.getHeaderId(objectId++), object, nodeConsumer::onObject);
							if (!sortedObject.hasNext()) {
								// no more objects, send the current subject and break the shared loop
								sendPiped(newSubject, CompressUtil.getHeaderId(subjectId++), subject, nodeConsumer::onSubject);
								break sharedLoop;
							}
							newObject = sortedObject.next();
							debugOrderCheckerO.accept(newObject);
						}
						comp = comparator.compare(newSubject.getNode(), newObject.getNode());
					}
					// shared element
					long shid = CompressUtil.asShared(sharedId++);
					nodeConsumer.onSubject(newSubject.getIndex(), shid);
					nodeConsumer.onObject(newObject.getIndex(), shid);
					shared.addElement(new CompactString(newSubject.getNode()));
				}
				// at least one iterator is empty, closing the shared pipe
				shared.closePipe();
				// do we have subjects?
				while (sortedSubject.hasNext()) {
					IndexedNode next = sortedSubject.next();
					debugOrderCheckerS.accept(next);
					sendPiped(next, CompressUtil.getHeaderId(subjectId++), subject, nodeConsumer::onSubject);
				}
				subject.closePipe();
				// do we have objects?
				while (sortedObject.hasNext()) {
					IndexedNode next = sortedObject.next();
					debugOrderCheckerO.accept(next);
					sendPiped(next, CompressUtil.getHeaderId(objectId++), object, nodeConsumer::onObject);
				}
				object.closePipe();
			} catch (Throwable t) {
				object.closePipe(t);
				subject.closePipe(t);
				shared.closePipe(t);
				throw t;
			}
		}, "CFSDPipeBuilder").startAll();

		// send to the consumer the element while parsing them
		this.subject = new OneReadDictionarySection(subject, subjects);
		this.predicate = new OneReadDictionarySection(new MapIterator<>(sortedPredicate.asIterator(), (node, index) -> {
			long header = CompressUtil.getHeaderId(index + 1);
			nodeConsumer.onPredicate(node.getIndex(), header);
			// force duplication because it's not made in a pipe like with the others
			return new CompactString(node.getNode());
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

	private interface NodeConsumerMethod {
		void consume(long id, long header);
	}
}
