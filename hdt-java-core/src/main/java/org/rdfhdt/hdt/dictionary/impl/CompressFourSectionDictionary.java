package org.rdfhdt.hdt.dictionary.impl;

import org.rdfhdt.hdt.dictionary.TempDictionary;
import org.rdfhdt.hdt.dictionary.TempDictionarySection;
import org.rdfhdt.hdt.dictionary.impl.section.OneReadDictionarySection;
import org.rdfhdt.hdt.enums.TripleComponentRole;
import org.rdfhdt.hdt.exceptions.NotImplementedException;
import org.rdfhdt.hdt.hdt.impl.diskimport.CompressionResult;
import org.rdfhdt.hdt.iterator.utils.MapIterator;
import org.rdfhdt.hdt.iterator.utils.SinglePipedIterator;
import org.rdfhdt.hdt.triples.IndexedNode;
import org.rdfhdt.hdt.triples.TempTriples;
import org.rdfhdt.hdt.util.io.compress.CompressUtil;
import org.rdfhdt.hdt.util.string.CharSequenceComparator;
import org.rdfhdt.hdt.util.string.ReplazableString;

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;

public class CompressFourSectionDictionary implements TempDictionary {
	private final TempDictionarySection subject;
	private final TempDictionarySection predicate;
	private final TempDictionarySection object;
	private final TempDictionarySection shared;

	private static void sendPiped(IndexedNode value, ReplazableString ref, SinglePipedIterator<IndexedNode> pipe) {
		pipe.addElement(() -> {
			ref.replace(value.getNode());
			return new IndexedNode(ref, value.getIndex());
		});
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
		SinglePipedIterator<IndexedNode> subject = new SinglePipedIterator<>();
		SinglePipedIterator<IndexedNode> object = new SinglePipedIterator<>();
		SinglePipedIterator<SharedNode> shared = new SinglePipedIterator<>();
		Comparator<CharSequence> comparator = CharSequenceComparator.getInstance();
		Thread readingThread = new Thread(() -> {
			ReplazableString pipedSubject = new ReplazableString();
			ReplazableString pipedObject = new ReplazableString();
			ReplazableString pipedShared = new ReplazableString();
			ReplazableString prePipedShared = new ReplazableString();

			sharedLoop:
			while (sortedObject.hasNext() && sortedSubject.hasNext()) {
				// last was a shared node
				IndexedNode newSubject = sortedSubject.next();
				IndexedNode newObject = sortedObject.next();
				int comp = comparator.compare(newSubject.getNode(), newObject.getNode());
				while (comp != 0) {
					if (comp < 0) {
						sendPiped(newSubject, pipedSubject, subject);
						if (!sortedSubject.hasNext()) {
							// no more subjects, send the current object and break the shared loop
							sendPiped(newObject, pipedObject, object);
							break sharedLoop;
						}
						newSubject = sortedSubject.next();
					} else {
						sendPiped(newObject, pipedObject, object);
						if (!sortedObject.hasNext()) {
							// no more objects, send the current subject and break the shared loop
							sendPiped(newSubject, pipedSubject, subject);
							break sharedLoop;
						}
						newObject = sortedObject.next();
					}
					comp = comparator.compare(newSubject.getNode(), newObject.getNode());
				}
				// shared element
				long indexSubject = newSubject.getIndex();
				long indexObject = newObject.getIndex();
				prePipedShared.replace(newSubject.getNode());
				shared.addElement(() -> {
					pipedShared.replace(prePipedShared);
					return new SharedNode(indexSubject, indexObject, pipedShared);
				});
			}
			// at least one iterator is empty, closing the shared pipe
			shared.closePipe();
			// do we have subjects?
			while (sortedSubject.hasNext()) {
				sendPiped(sortedSubject.next(), pipedSubject, subject);
			}
			subject.closePipe();
			// do we have objects?
			while (sortedObject.hasNext()) {
				sendPiped(sortedObject.next(), pipedObject, object);
			}
			object.closePipe();
		}, "CFSDPipeBuilder");
		readingThread.start();

		// send to the consumer the element while parsing them
		this.subject = new OneReadDictionarySection(subject.mapWithId((node, index) -> {
			nodeConsumer.onSubject(node.getIndex(), index);
			return node.getNode();
		}), subjects);
		this.predicate = new OneReadDictionarySection(new MapIterator<>(sortedPredicate, (node, index) -> {
			nodeConsumer.onPredicate(node.getIndex(), index);
			return node.getNode();
		}), predicates);
		this.object = new OneReadDictionarySection(object.mapWithId((node, index) -> {
			nodeConsumer.onObject(node.getIndex(), index);
			return node.getNode();
		}), objects);
		this.shared = new OneReadDictionarySection(shared.mapWithId((node, index) -> {
			long sharedIndex = CompressUtil.asShared(index);
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
