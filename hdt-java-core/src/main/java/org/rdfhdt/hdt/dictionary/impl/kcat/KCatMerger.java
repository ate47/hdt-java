package org.rdfhdt.hdt.dictionary.impl.kcat;

import org.rdfhdt.hdt.compact.sequence.SequenceLog64BigDisk;
import org.rdfhdt.hdt.dictionary.DictionaryFactory;
import org.rdfhdt.hdt.dictionary.DictionaryKCat;
import org.rdfhdt.hdt.dictionary.DictionaryPrivate;
import org.rdfhdt.hdt.dictionary.DictionarySection;
import org.rdfhdt.hdt.dictionary.DictionarySectionPrivate;
import org.rdfhdt.hdt.dictionary.impl.section.OneReadDictionarySection;
import org.rdfhdt.hdt.dictionary.impl.section.WriteDictionarySection;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.iterator.utils.ExceptionIterator;
import org.rdfhdt.hdt.iterator.utils.MapIterator;
import org.rdfhdt.hdt.iterator.utils.MergeExceptionIterator;
import org.rdfhdt.hdt.iterator.utils.PipedCopyIterator;
import org.rdfhdt.hdt.listener.ProgressListener;
import org.rdfhdt.hdt.options.HDTOptions;
import org.rdfhdt.hdt.triples.TripleID;
import org.rdfhdt.hdt.util.BitUtil;
import org.rdfhdt.hdt.util.LiteralsUtils;
import org.rdfhdt.hdt.util.concurrent.ExceptionThread;
import org.rdfhdt.hdt.util.concurrent.SyncSeq;
import org.rdfhdt.hdt.util.io.CloseSuppressPath;
import org.rdfhdt.hdt.util.io.Closer;
import org.rdfhdt.hdt.util.string.ByteString;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Class to merge multiple dictionaries into S/O/SH streams with map writing
 *
 * @author Antoine Willerval
 */
public class KCatMerger implements AutoCloseable {
	private static final long SHARED_MASK = 0b01;
	private static final long TYPED_MASK = 0b10;

	final HDT[] hdts;

	private final ProgressListener listener;
	private final CloseSuppressPath[] locations;
	final SyncSeq[] subjectsMaps;
	final SyncSeq[] predicatesMaps;
	final SyncSeq[] objectsMaps;
	private final ExceptionThread catMergerThread;
	final boolean typedHDT;
	private final int shift;
	private final String dictionaryType;

	private final PipedCopyIterator<DuplicateBuffer> subjectPipe = new PipedCopyIterator<>();
	private final PipedCopyIterator<DuplicateBuffer> objectPipe = new PipedCopyIterator<>();
	private final PipedCopyIterator<BiDuplicateBuffer> sharedPipe = new PipedCopyIterator<>();
	private final DuplicateBufferIterator<RuntimeException> sortedSubject;
	private final DuplicateBufferIterator<RuntimeException> sortedObject;
	private final DuplicateBufferIterator<RuntimeException> sortedPredicates;
	private final Map<ByteString, DuplicateBufferIterator<RuntimeException>> sortedSubSections;

	private final long estimatedSizeP;
	private final AtomicLong countTyped = new AtomicLong();
	private final AtomicLong countShared = new AtomicLong();
	final AtomicLong[] countSubject;
	final AtomicLong[] countObject;

	private final WriteDictionarySection sectionSubject;
	private final WriteDictionarySection sectionShared;
	private final WriteDictionarySection sectionObject;
	private final WriteDictionarySection sectionPredicate;
	private final Map<ByteString, WriteDictionarySection> sectionSub;
	private final Map<ByteString, Integer> typeId = new HashMap<>();
	private boolean running;

	/**
	 * Create KCatMerger
	 *
	 * @param hdts           the hdts to cat
	 * @param location       working location
	 * @param listener       listener to log the state
	 * @param bufferSize     buffer size
	 * @param dictionaryType dictionary type
	 * @param spec           spec to config the HDT
	 * @throws java.io.IOException io exception
	 */
	public KCatMerger(HDT[] hdts, CloseSuppressPath location, ProgressListener listener, int bufferSize, String dictionaryType, HDTOptions spec) throws IOException {
		this.hdts = hdts;
		this.listener = listener;
		this.dictionaryType = dictionaryType;

		DictionaryKCat[] cats = new DictionaryKCat[hdts.length];
		subjectsMaps = new SyncSeq[hdts.length];
		predicatesMaps = new SyncSeq[hdts.length];
		objectsMaps = new SyncSeq[hdts.length];
		locations = new CloseSuppressPath[hdts.length * 3];

		countSubject = IntStream.range(0, hdts.length).mapToObj(i -> new AtomicLong()).toArray(AtomicLong[]::new);
		countObject = IntStream.range(0, hdts.length).mapToObj(i -> new AtomicLong()).toArray(AtomicLong[]::new);

		long sizeS = 0;
		long sizeP = 0;
		long sizeO = 0;
		long sizeShared = 0;

		Map<ByteString, PreIndexSection[]> subSections = new TreeMap<>();

		for (int i = 0; i < cats.length; i++) {
			DictionaryKCat cat = DictionaryFactory.createDictionaryKCat(hdts[i].getDictionary());

			// compute max allocated sizes
			sizeS += cat.countSubjects();
			sizeP += cat.countPredicates();
			sizeO += cat.countObjects();
			sizeShared += cat.countShared();

			long start = 1L + cat.countShared();
			// compute allocated sizes for HDT with sub sections
			for (Map.Entry<CharSequence, DictionarySection> e : cat.getSubSections().entrySet()) {
				CharSequence key = e.getKey();
				DictionarySection section = e.getValue();

				PreIndexSection[] sections = subSections.computeIfAbsent(
						ByteString.of(key),
						k -> new PreIndexSection[cats.length]
				);
				sections[i] = new PreIndexSection(start, section);
				start += section.getNumberOfElements();
			}
			cats[i] = cat;
		}
		// if this HDT is typed, we don't have to allocate 1 bit / node to note a typed node
		this.typedHDT = !subSections.isEmpty();
		if (typedHDT) {
			shift = 2;
		} else {
			shift = 1;
		}

		this.estimatedSizeP = sizeP;
		try {
			// create maps, allocate more bits for the shared part
			int numbitsS = BitUtil.log2(sizeS + 1 + sizeShared) + 1 + shift;
			int numbitsP = BitUtil.log2(sizeP + 1);
			int numbitsO = BitUtil.log2(sizeO + 1 + sizeShared) + 1 + shift;
			for (int i = 0; i < cats.length; i++) {
				DictionaryKCat cat = cats[i];
				subjectsMaps[i] = new SyncSeq(new SequenceLog64BigDisk((locations[i * 3] = location.resolve("subjectsMap_" + i)).toAbsolutePath().toString(), numbitsS, cat.countSubjects() + 1));
				predicatesMaps[i] = new SyncSeq(new SequenceLog64BigDisk((locations[i * 3 + 1] = location.resolve("predicatesMap_" + i)).toAbsolutePath().toString(), numbitsP, cat.countPredicates() + 1));
				objectsMaps[i] = new SyncSeq(new SequenceLog64BigDisk((locations[i * 3 + 2] = location.resolve("objectsMap_" + i)).toAbsolutePath().toString(), numbitsO, cat.countObjects() + 1));
			}

			// merge the subjects/objects/shared from all the HDTs
			sortedSubject = mergeSection(
					cats,
					(hdtIndex, c) -> createMergeIt(
							hdtIndex,
							c.getSubjectSection().getSortedEntries(),
							c.getSharedSection().getSortedEntries(),
							c.countShared()
					)
			);
			sortedObject = mergeSection(
					cats,
					(hdtIndex, c) -> createMergeIt(
							hdtIndex,
							c.getObjectSection().getSortedEntries(),
							c.getSharedSection().getSortedEntries(),
							c.objectShift()
					)
			);

			// merge the other sections
			sortedPredicates = mergeSection(cats, (hdtIndex, c) -> {
				ExceptionIterator<? extends CharSequence, RuntimeException> of = ExceptionIterator.of(c.getPredicateSection().getSortedEntries());
				return of.map(((element, index) -> new LocatedIndexedNode(hdtIndex, index + 1, ByteString.of(element))));
			});

			sortedSubSections = new TreeMap<>();
			// create a merge section for each section
			subSections.forEach((key, sections) -> sortedSubSections.put(key, mergeSection(sections, (hdtIndex, pre) -> {
				ExceptionIterator<? extends CharSequence, RuntimeException> of = ExceptionIterator.of(pre.getSortedEntries());
				return of.map(((element, index) -> new LocatedIndexedNode(hdtIndex, pre.getStart() + index, ByteString.of(element))));
			})));

			// convert the dupe buffer streams to byte string streams

			Iterator<ByteString> subject = subjectPipe.mapWithId((db, id) -> {
				long header = withEmptyHeader(id + 1);
				db.stream().forEach(node -> {
					SyncSeq map = subjectsMaps[node.getHdt()];
					assert map.get(node.getIndex()) == 0 : "overwriting previous subject value";
					map.set(node.getIndex(), header);
					countSubject[node.getHdt()].incrementAndGet();
				});
				return db.peek();
			});

			Iterator<ByteString> object = objectPipe.mapWithId((db, id) -> {
				long header = withEmptyHeader(id + 1);
				db.stream().forEach(node -> {
					SyncSeq map = objectsMaps[node.getHdt()];
					assert map.get(node.getIndex()) == 0 : "overwriting previous object value";
					assert node.getIndex() >= 1 && node.getIndex() <= hdts[node.getHdt()].getDictionary().getNobjects();
					map.set(node.getIndex(), header);
					countObject[node.getHdt()].incrementAndGet();
				});
				return db.peek();
			});

			// left = subjects
			// right = objects
			Iterator<ByteString> shared = sharedPipe.mapWithId((bdb, id) -> {
				long header = withSharedHeader(id + 1);
				countShared.incrementAndGet();
				// left = subjects
				bdb.getLeft().stream().forEach(node -> {
					SyncSeq map = subjectsMaps[node.getHdt()];
					assert map.get(node.getIndex()) == 0 : "overwriting previous subject value";
					map.set(node.getIndex(), header);
					countSubject[node.getHdt()].incrementAndGet();
				});
				// right = objects
				bdb.getRight().stream().forEach(node -> {
					SyncSeq map = objectsMaps[node.getHdt()];
					assert map.get(node.getIndex()) == 0 : "overwriting previous object value";
					assert node.getIndex() >= 1 && node.getIndex() <= hdts[node.getHdt()].getDictionary().getNobjects();
					map.set(node.getIndex(), header);
					countObject[node.getHdt()].incrementAndGet();
				});
				return bdb.peek();
			});

			sectionSubject = new WriteDictionarySection(spec, location.resolve("sortedSubject"), bufferSize);
			sectionShared = new WriteDictionarySection(spec, location.resolve("sortedShared"), bufferSize);
			sectionObject = new WriteDictionarySection(spec, location.resolve("sortedObject"), bufferSize);
			sectionPredicate = new WriteDictionarySection(spec, location.resolve("sortedPredicate"), bufferSize);
			sectionSub = new TreeMap<>();
			sortedSubSections.keySet().forEach((key) -> sectionSub.put(key, new WriteDictionarySection(spec, location.resolve("sortedSub" + getTypeId(key)), bufferSize)));

			catMergerThread = new ExceptionThread(this::runSharedCompute, "KCatMergerThreadShared")
					.attach(new ExceptionThread(this::runSubSectionCompute, "KCatMergerThreadSubSection"))
					.attach(new ExceptionThread(createWriter(sectionSubject, sizeS, subject), "KCatMergerThreadWriterS"))
					.attach(new ExceptionThread(createWriter(sectionShared, sizeS + sizeO - sizeShared, shared), "KCatMergerThreadWriterSH"))
					.attach(new ExceptionThread(createWriter(sectionObject, sizeO, object), "KCatMergerThreadWriterO"));
		} catch (Throwable t) {
			try {
				throw t;
			} finally {
				close();
			}
		}
	}

	private static ExceptionIterator<LocatedIndexedNode, RuntimeException> createMergeIt(int hdtIndex, Iterator<? extends CharSequence> subjectObject, Iterator<? extends CharSequence> shared, long sharedCount) {
		return MergeExceptionIterator.buildOfTree(List.of(
				MapIterator.of(subjectObject, (element, index) ->
								new LocatedIndexedNode(hdtIndex, sharedCount + index + 1, ByteString.of(element)))
						.asExceptionIterator(),
				MapIterator.of(shared, (element, index) ->
								new LocatedIndexedNode(hdtIndex, index + 1, ByteString.of(element)))
						.asExceptionIterator()
		));
	}

	/**
	 * create a sorted LocatedIndexedNode iterator from an array of sections
	 *
	 * @param sections the sections
	 * @return iterator
	 */
	public static <T> DuplicateBufferIterator<RuntimeException> mergeSection(T[] sections, MergerFunction<T> mapper) {
		return new DuplicateBufferIterator<>(
				MergeExceptionIterator
						.buildOfTree(
								(Integer hdtIndex, T e) -> {
									if (e == null) {
										// empty section (not defined for this HDT)
										return ExceptionIterator.empty();
									}
									// convert all the entries into located nodes
									return mapper.apply(hdtIndex, e);
								},
								LocatedIndexedNode::compareTo,
								Arrays.asList(sections),
								0,
								sections.length
						),
				sections.length
		);
	}

	/**
	 * get an UID for a type
	 *
	 * @param str the type
	 * @return UID
	 */
	public int getTypeId(ByteString str) {
		return typeId.computeIfAbsent(str, (key) -> typeId.size());
	}

	/**
	 * add a typed header to this value
	 *
	 * @param value value
	 * @return header value
	 * @see #withEmptyHeader(long)
	 * @see #withSharedHeader(long)
	 */
	public long withTypedHeader(long value) {
		assert value != 0 : "value can't be 0!";
		return (value << shift) | TYPED_MASK;
	}

	/**
	 * add a shared header to this value
	 *
	 * @param value value
	 * @return header value
	 * @see #withEmptyHeader(long)
	 * @see #withTypedHeader(long)
	 */
	public long withSharedHeader(long value) {
		assert value != 0 : "value can't be 0!";
		return (value << shift) | SHARED_MASK;
	}

	/**
	 * add a header to this value
	 *
	 * @param value value
	 * @return header value
	 * @see #withTypedHeader(long)
	 * @see #withSharedHeader(long)
	 */
	public long withEmptyHeader(long value) {
		assert value != 0 : "value can't be 0!";
		return value << shift;
	}

	boolean assertReadCorrectly() {
		for (int i = 0; i < hdts.length; i++) {
			HDT hdt = hdts[i];
			assert countObject[i].get() == hdt.getDictionary().getNobjects();
			assert countSubject[i].get() == hdt.getDictionary().getNsubjects();
		}
		return true;
	}

	/**
	 * test if a header value is shared
	 *
	 * @param headerValue header value
	 * @return true if the header is shared, false otherwise
	 */
	public boolean isShared(long headerValue) {
		return (headerValue & SHARED_MASK) != 0;
	}

	/**
	 * test if a header value is typed
	 *
	 * @param headerValue header value
	 * @return true if the header is typed, false otherwise
	 */
	public boolean isTyped(long headerValue) {
		return typedHDT && (headerValue & TYPED_MASK) != 0;
	}

	/**
	 * wait for the merger to complete
	 *
	 * @throws InterruptedException thread interruption
	 */
	public DictionaryPrivate buildDictionary() throws InterruptedException {
		synchronized (this) {
			if (!running) {
				startMerger();
			}
		}
		catMergerThread.joinAndCrashIfRequired();

		return DictionaryFactory.createWriteDictionary(
				dictionaryType,
				null,
				getSectionSubject(),
				getSectionPredicate(),
				getSectionObject(),
				getSectionShared(),
				getSectionSub()
		);
	}

	private void runSharedCompute() {
		// merge the sections
		try {
			sharedLoop:
			while (sortedObject.hasNext() && sortedSubject.hasNext()) {
				// last was a shared node
				DuplicateBuffer newSubject = sortedSubject.next();
				DuplicateBuffer newObject = sortedObject.next();
				int comp = newSubject.compareTo(newObject);
				while (comp != 0) {
					if (comp < 0) {
						subjectPipe.addElement(newSubject.trim());
						if (!sortedSubject.hasNext()) {
							// no more subjects, send the current object and break the shared loop
							objectPipe.addElement(newObject.trim());
							break sharedLoop;
						}
						newSubject = sortedSubject.next();
					} else {
						objectPipe.addElement(newObject.trim());
						if (!sortedObject.hasNext()) {
							// no more objects, send the current subject and break the shared loop
							subjectPipe.addElement(newSubject.trim());
							break sharedLoop;
						}
						newObject = sortedObject.next();
					}
					comp = newSubject.compareTo(newObject);
				}

				// shared element
				sharedPipe.addElement(newSubject.trim().asBi(newObject.trim()));
			}

			// at least one iterator is empty, closing the shared pipe
			sharedPipe.closePipe();
			// do we have subjects?
			while (sortedSubject.hasNext()) {
				subjectPipe.addElement(sortedSubject.next().trim());
			}
			subjectPipe.closePipe();
			// do we have objects?
			while (sortedObject.hasNext()) {
				objectPipe.addElement(sortedObject.next().trim());
			}
			objectPipe.closePipe();
		} catch (Throwable t) {
			objectPipe.closePipe(t);
			subjectPipe.closePipe(t);
			sharedPipe.closePipe(t);
			throw t;
		}
	}

	private void runSubSectionCompute() {
		// convert all the sections

		// load predicates
		sectionPredicate.load(new OneReadDictionarySection(sortedPredicates.map((db, id) -> {
			db.stream().forEach(node -> {
				SyncSeq map = predicatesMaps[node.getHdt()];
				assert map.get(node.getIndex()) == 0 : "overwriting previous predicate value";
				map.set(node.getIndex(), id + 1);
			});
			return db.peek();
		}).asIterator(), estimatedSizeP), null);

		long shift = 1L;
		// load data typed sections
		for (Map.Entry<ByteString, WriteDictionarySection> e : sectionSub.entrySet()) {
			ByteString key = e.getKey();
			WriteDictionarySection section = e.getValue();

			DuplicateBufferIterator<RuntimeException> bufferIterator = sortedSubSections.get(key);

			final long currentShift = shift;
			section.load(new OneReadDictionarySection(bufferIterator.map((db, id) -> {
				long headerID = withTypedHeader(id + currentShift);
				countTyped.incrementAndGet();
				db.stream().forEach(node -> {
					SyncSeq map = objectsMaps[node.getHdt()];
					assert map.get(node.getIndex()) == 0 : "overwriting previous object value";
					assert node.getIndex() >= 1 && node.getIndex() <= hdts[node.getHdt()].getDictionary().getNobjects();
					map.set(node.getIndex(), headerID);
					countObject[node.getHdt()].incrementAndGet();
				});
				return db.peek();
			}).asIterator(), estimatedSizeP), null);
			shift += section.getNumberOfElements();
		}
	}

	private ExceptionThread.ExceptionRunnable createWriter(DictionarySectionPrivate sect, long size, Iterator<ByteString> iterator) {
		// convert all the sections
		return () -> sect.load(new OneReadDictionarySection(iterator, size), listener);
	}

	@Override
	public void close() throws IOException {
		try {
			if (catMergerThread != null) {
				catMergerThread.joinAndCrashIfRequired();
			}
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		} finally {
			Closer.of(sectionSubject, sectionPredicate, sectionObject, sectionShared)
					.with(sectionSub == null ? List.of() : sectionSub.values())
					.with(subjectsMaps)
					.with(predicatesMaps)
					.with(objectsMaps)
					.with(locations)
					.close();
		}
	}

	/**
	 * remove the header of a header id
	 *
	 * @param headerID header id
	 * @return id
	 */
	public long removeHeader(long headerID) {
		return headerID >>> shift;
	}

	/**
	 * extract the subject from an HDT
	 *
	 * @param hdtIndex the HDT index
	 * @param oldID    the ID in the HDT triples
	 * @return ID in the new HDT
	 */
	public long extractSubject(int hdtIndex, long oldID) {
		long headerID = subjectsMaps[hdtIndex].get(oldID);
		if (isShared(headerID)) {
			return headerID >>> shift;
		}
		return (headerID >>> shift) + countShared.get();
	}

	/**
	 * extract the predicate from an HDT
	 *
	 * @param hdtIndex the HDT index
	 * @param oldID    the ID in the HDT triples
	 * @return ID in the new HDT
	 */
	public long extractPredicate(int hdtIndex, long oldID) {
		return predicatesMaps[hdtIndex].get(oldID);
	}

	/**
	 * extract the object from an HDT
	 *
	 * @param hdtIndex the HDT index
	 * @param oldID    the ID in the HDT triples
	 * @return ID in the new HDT
	 */
	public long extractObject(int hdtIndex, long oldID) {
		long headerID = objectsMaps[hdtIndex].get(oldID);
		if (isShared(headerID)) {
			return headerID >>> shift;
		}
		if (isTyped(headerID)) {
			return (headerID >>> shift) + countShared.get();
		}
		return (headerID >>> shift) + countShared.get() + countTyped.get();
	}

	/**
	 * copy into a new TripleID the mapped version of a tripleID
	 *
	 * @param hdtIndex the origin HDT of this tripleID
	 * @param id       the tripleID
	 * @return mapped tripleID
	 */
	public TripleID extractMapped(int hdtIndex, TripleID id) {
		TripleID mapped = new TripleID(
				extractSubject(hdtIndex, id.getSubject()),
				extractPredicate(hdtIndex, id.getPredicate()),
				extractObject(hdtIndex, id.getObject())
		);
		assert mapped.isValid() : "mapped to empty triples! " + id + " => " + mapped;
		return mapped;
	}

	/**
	 * @return the count of shared elements
	 */
	public long getCountShared() {
		return countShared.get();
	}

	/**
	 * @return subject section
	 */
	public DictionarySectionPrivate getSectionSubject() {
		return sectionSubject;
	}

	/**
	 * @return shared section
	 */
	public DictionarySectionPrivate getSectionShared() {
		return sectionShared;
	}

	/**
	 * @return object section
	 */
	public DictionarySectionPrivate getSectionObject() {
		return sectionObject;
	}

	/**
	 * @return predicate section
	 */
	public DictionarySectionPrivate getSectionPredicate() {
		return sectionPredicate;
	}

	/**
	 * @return sub sections
	 */
	public TreeMap<ByteString, DictionarySectionPrivate> getSectionSub() {
		TreeMap<ByteString, DictionarySectionPrivate> sub = new TreeMap<>(sectionSub);
		sub.put(LiteralsUtils.NO_DATATYPE, getSectionObject());
		return sub;
	}

	/**
	 * start the merger threads
	 */
	public synchronized void startMerger() {
		if (running) {
			throw new IllegalArgumentException("KCatMerger is already running!");
		}
		running = true;

		catMergerThread.startAll();
	}

	static class BiDuplicateBuffer implements Comparable<BiDuplicateBuffer> {
		private final DuplicateBuffer left;
		private final DuplicateBuffer right;


		public BiDuplicateBuffer(DuplicateBuffer left, DuplicateBuffer right) {
			this.left = Objects.requireNonNull(left, "left buffer can't be null!");
			this.right = Objects.requireNonNull(right, "right buffer can't be null!");
			assert left.isEmpty() || right.isEmpty() || left.peek().equals(right.peek()) : "Can't have heterogeneous bi dupe buffer";
		}

		public DuplicateBuffer getLeft() {
			return left;
		}

		public DuplicateBuffer getRight() {
			return right;
		}

		public boolean isEmpty() {
			return getLeft().isEmpty() && getRight().isEmpty();
		}

		public ByteString peek() {
			if (!left.isEmpty()) {
				return left.peek();
			}
			if (!right.isEmpty()) {
				return right.peek();
			}
			return null;
		}

		@Override
		public int compareTo(BiDuplicateBuffer o) {
			return peek().compareTo(o.peek());
		}
	}

	static class DuplicateBuffer implements Comparable<DuplicateBuffer> {
		private final LocatedIndexedNode[] buffer;
		private int used;

		public DuplicateBuffer(int bufferSize) {
			this.buffer = new LocatedIndexedNode[bufferSize];
		}

		/**
		 * add a node to this buffer
		 *
		 * @param node node
		 * @return if this node was added to the buffer
		 */
		private boolean add(LocatedIndexedNode node) {
			// start case
			if (isEmpty() || buffer[0].getNode().equals(node.getNode())) {
				// we can't have more than buffer size because a source HDT wouldn't be
				// without duplicated or a so/sh conflict
				buffer[used++] = node;
				return true;
			}

			return false;
		}

		/**
		 * convert this buffer to a bi duplicate buffer
		 *
		 * @param other right part
		 * @return BiDuplicateBuffer
		 */
		public BiDuplicateBuffer asBi(DuplicateBuffer other) {
			return new BiDuplicateBuffer(this, other);
		}

		/**
		 * @return if this buffer contains at least one element
		 */
		public boolean isEmpty() {
			return used == 0;
		}

		/**
		 * clear the buffer
		 */
		public void clear() {
			// clear old values
			for (int i = 0; i < used; i++) {
				buffer[i] = null;
			}
			used = 0;
		}

		/**
		 * @return a stream of the current duplicate objects
		 */
		public Stream<LocatedIndexedNode> stream() {
			return Arrays.stream(buffer, 0, used);
		}

		@Override
		public int compareTo(DuplicateBuffer o) {
			if (isEmpty() || o.isEmpty()) {
				throw new IllegalArgumentException("Can't compare empty buffers");
			}
			return buffer[0].compareTo(o.buffer[0]);
		}

		/**
		 * @return a trimmed version of this buffer
		 */
		public DuplicateBuffer trim() {
			DuplicateBuffer other = new DuplicateBuffer(used);
			System.arraycopy(buffer, 0, other.buffer, 0, used);
			other.used = used;
			return other;
		}

		/**
		 * @return the buffered byte string, null if empty
		 */
		public ByteString peek() {
			if (isEmpty()) {
				return null;
			}
			return buffer[0].getNode();
		}

		/**
		 * @return the size of the buffer
		 */
		public int size() {
			return used;
		}
	}

	static class DuplicateBufferIterator<E extends Exception> implements ExceptionIterator<DuplicateBuffer, E> {
		private final ExceptionIterator<LocatedIndexedNode, E> iterator;
		private final DuplicateBuffer buffer;
		private LocatedIndexedNode last;
		private DuplicateBuffer next;

		public DuplicateBufferIterator(ExceptionIterator<LocatedIndexedNode, E> iterator, int bufferSize) {
			this.iterator = iterator;
			buffer = new DuplicateBuffer(bufferSize);
		}

		@Override
		public boolean hasNext() throws E {
			if (next != null) {
				return true;
			}

			// clear previous buffer
			buffer.clear();
			while (true) {
				// load an element from the iterator
				if (last == null) {
					if (!iterator.hasNext()) {
						if (buffer.isEmpty()) {
							return false;
						}
						break;
					}
					last = iterator.next();
				}

				// add the elements from the iterator
				if (!buffer.add(last)) {
					break;
				}
				last = null;
			}

			next = buffer.trim();
			return true;
		}

		@Override
		public DuplicateBuffer next() throws E {
			if (!hasNext()) {
				return null;
			}
			try {
				return next;
			} finally {
				next = null;
			}
		}

	}

	private interface MergerFunction<T> {
		ExceptionIterator<LocatedIndexedNode, RuntimeException> apply(int hdtIndex, T t);
	}

	private static class PreIndexSection {
		long start;
		DictionarySection section;

		public PreIndexSection(long start, DictionarySection section) {
			this.start = start;
			this.section = section;
		}

		public long getStart() {
			return start;
		}

		public DictionarySection getSection() {
			return section;
		}

		public Iterator<? extends CharSequence> getSortedEntries() {
			return getSection().getSortedEntries();
		}
	}
}
