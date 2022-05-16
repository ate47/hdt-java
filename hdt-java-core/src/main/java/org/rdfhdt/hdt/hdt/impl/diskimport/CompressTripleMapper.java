package org.rdfhdt.hdt.hdt.impl.diskimport;

import org.rdfhdt.hdt.compact.sequence.Sequence;
import org.rdfhdt.hdt.compact.sequence.SequenceLog64BigDisk;
import org.rdfhdt.hdt.dictionary.impl.CompressFourSectionDictionary;
import org.rdfhdt.hdt.util.BitUtil;
import org.rdfhdt.hdt.util.disk.LongArrayDisk;
import org.rdfhdt.hdt.util.io.CloseSuppressPath;
import org.rdfhdt.hdt.util.io.IOUtil;
import org.rdfhdt.hdt.util.io.compress.CompressUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Map a compress triple file to long array map files
 *
 * @author Antoine Willerval
 */
public class CompressTripleMapper implements CompressFourSectionDictionary.NodeConsumer {
	private static final Logger log = LoggerFactory.getLogger(CompressTripleMapper.class);
	private final SequenceLog64BigDisk subjects;
	private final SequenceLog64BigDisk predicates;
	private final SequenceLog64BigDisk objects;
	private final CloseSuppressPath locationSubjects;
	private final CloseSuppressPath locationPredicates;
	private final CloseSuppressPath locationObjects;
	private long shared = -1;

	public CompressTripleMapper(CloseSuppressPath location, long tripleCount) {
		locationSubjects = location.resolve("map_subjects");
		locationPredicates = location.resolve("map_predicates");
		locationObjects = location.resolve("map_objects");
		int numbits = BitUtil.log2(tripleCount + 2) + CompressUtil.INDEX_SHIFT;
		subjects = new SequenceLog64BigDisk(locationSubjects.toAbsolutePath().toString(), numbits, tripleCount + 2);
		predicates = new SequenceLog64BigDisk(locationPredicates.toAbsolutePath().toString(), numbits, tripleCount + 2);
		objects = new SequenceLog64BigDisk(locationObjects.toAbsolutePath().toString(), numbits, tripleCount + 2);
	}

	/**
	 * delete the map files and the location files
	 */
	public void delete() {
		try {
			IOUtil.closeAll(subjects, predicates, objects);
		} catch (IOException e) {
			log.warn("Can't close triple map array", e);
		}
		try {
			IOUtil.closeAll(locationSubjects, locationPredicates, locationObjects);
		} catch (IOException e) {
			log.warn("Can't delete triple map array files", e);
		}
	}

	@Override
	public void onSubject(long preMapId, long newMapId) {
		subjects.set(preMapId, newMapId);
	}

	@Override
	public void onPredicate(long preMapId, long newMapId) {
		predicates.set(preMapId, newMapId);
	}

	@Override
	public void onObject(long preMapId, long newMapId) {
		objects.set(preMapId, newMapId);
	}

	public Sequence getSubjects() {
		return subjects;
	}

	public Sequence getPredicates() {
		return predicates;
	}

	public Sequence getObjects() {
		return objects;
	}

	public void setShared(long shared) {
		this.shared = shared;
	}

	private void checkShared() {
		if (this.shared < 0) {
			throw new IllegalArgumentException("Shared not set!");
		}
	}

	/**
	 * extract the map id of a subject
	 *
	 * @param id id
	 * @return new id
	 */
	public long extractSubject(long id) {
		return extract(subjects, id);
	}

	/**
	 * extract the map id of a predicate
	 *
	 * @param id id
	 * @return new id
	 */
	public long extractPredicate(long id) {
		return extract(predicates, id) - shared;
	}

	/**
	 * extract the map id of a object
	 *
	 * @param id id
	 * @return new id
	 */
	public long extractObjects(long id) {
		return extract(objects, id);
	}

	private long extract(Sequence array, long id) {
		checkShared();
		long data = array.get(id);
		// loop over the duplicates
		while (CompressUtil.isDuplicated(data)) {
			long remap = array.get(CompressUtil.getId(data));
			assert remap != data : "remap and data are the same!";
			data = remap;
		}
		// compute shared if required
		return CompressUtil.computeSharedNode(data, shared);
	}
}
