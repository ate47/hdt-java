package org.rdfhdt.hdt.util.io.compress;

public class MappedId {
	private long tripleId;
	private long mappedId;

	public MappedId(long tripleId, long mappedId) {
		setAll(tripleId, mappedId);
	}

	public MappedId() {
		this(0, 0);
	}

	public void setAll(long tripleId, long mappedId) {
		this.tripleId = tripleId;
		this.mappedId = mappedId;
	}

	public long getMappedId() {
		return mappedId;
	}

	public long getTripleId() {
		return tripleId;
	}
}
