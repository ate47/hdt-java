package org.rdfhdt.hdt.hdt.impl.diskimport;

import org.rdfhdt.hdt.dictionary.impl.CompressFourSectionDictionary;
import org.rdfhdt.hdt.util.disk.LongArrayDisk;
import org.rdfhdt.hdt.util.io.compress.CompressUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

public class CompressTripleMapper implements CompressFourSectionDictionary.NodeConsumer {
	private static final Logger log = LoggerFactory.getLogger(CompressTripleMapper.class);
	private final LongArrayDisk subjects;
	private final LongArrayDisk predicates;
	private final LongArrayDisk objects;
	private final File locationSubjects;
	private final File locationPredicates;
	private final File locationObjects;
	private long shared = -1;

	public CompressTripleMapper(String location, long tripleCount) {
		File l = new File(location);
		locationSubjects = new File(l, "map_subjects");
		locationPredicates = new File(l, "map_predicates");
		locationObjects = new File(l, "map_objects");
		subjects = new LongArrayDisk(locationSubjects.getAbsolutePath(), tripleCount);
		predicates = new LongArrayDisk(locationPredicates.getAbsolutePath(), tripleCount);
		objects = new LongArrayDisk(locationObjects.getAbsolutePath(), tripleCount);
	}

	public void delete() {
		try {
			try {
				subjects.close();
			} finally {
				try {
					predicates.close();
				} finally {
					objects.close();
				}
			}
		} catch (IOException e) {
			log.warn("Can't close triple map array", e);
		}
		try {
			try {
				Files.deleteIfExists(locationSubjects.toPath());
			} finally {
				try {
					Files.deleteIfExists(locationPredicates.toPath());
				} finally {
					Files.deleteIfExists(locationObjects.toPath());
				}
			}
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

	public LongArrayDisk getSubjects() {
		return subjects;
	}

	public LongArrayDisk getPredicates() {
		return predicates;
	}

	public LongArrayDisk getObjects() {
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
	 * @param id id
	 * @return new id
	 */
	public long extractSubject(long id) {
		return extract(subjects, id);
	}

	/**
	 * extract the map id of a predicate
	 * @param id id
	 * @return new id
	 */
	public long extractPredicate(long id) {
		return extract(predicates, id);
	}

	/**
	 * extract the map id of a object
	 * @param id id
	 * @return new id
	 */
	public long extractObjects(long id) {
		return extract(objects, id);
	}

	private long extract(LongArrayDisk array, long id) {
		checkShared();
		long data = array.get(id);
		// loop over the duplicates
		while (CompressUtil.isDuplicated(data)) {
			data = array.get(CompressUtil.getDuplicatedIndex(data));
		}
		// compute shared if required
		return CompressUtil.computeSharedNode(data, shared);
	}
}
