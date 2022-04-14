package org.rdfhdt.hdt.triples;

import org.rdfhdt.hdt.util.string.CharSequenceComparator;

import java.util.Comparator;

public class IndexedNode implements Comparable<IndexedNode> {
	public static final IndexedNode UNKNOWN = new IndexedNode("UNKNOWN", -1);
	private static final Comparator<CharSequence> NODE_COMPARATOR = CharSequenceComparator.getInstance();
	private final CharSequence node;
	private final long index;

	public IndexedNode(CharSequence node, long index) {
		this.node = node;
		this.index = index;
	}

	public CharSequence getNode() {
		return node;
	}

	public long getIndex() {
		return index;
	}

	@Override
	public int compareTo(IndexedNode o) {
		return NODE_COMPARATOR.compare(node, o.getNode());
	}
}
