package org.rdfhdt.hdt.triples.impl.utils;

import org.junit.Assert;
import org.rdfhdt.hdt.dictionary.Dictionary;
import org.rdfhdt.hdt.dictionary.DictionarySection;
import org.rdfhdt.hdt.enums.TripleComponentRole;
import org.rdfhdt.hdt.exceptions.NotFoundException;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdt.hdt.HDTVocabulary;
import org.rdfhdt.hdt.hdt.writer.TripleWriterHDT;
import org.rdfhdt.hdt.options.HDTOptions;
import org.rdfhdt.hdt.options.HDTOptionsKeys;
import org.rdfhdt.hdt.options.HDTSpecification;
import org.rdfhdt.hdt.quad.QuadString;
import org.rdfhdt.hdt.triples.IteratorTripleString;
import org.rdfhdt.hdt.triples.TripleID;
import org.rdfhdt.hdt.triples.TripleString;
import org.rdfhdt.hdt.util.string.CharSequenceComparator;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/**
 * class to generate a synthetic hdt for test purposes.
 * @author ate47
 */
public class HDTTestUtils implements Closeable {

    /**
     * base URI
     */
    public static final String BASE_URI = "http://ex.org/";

    public static class Tuple<T1, T2> {
        public final T1 t1;
        public final T2 t2;

        public Tuple(T1 t1, T2 t2) {
            this.t1 = t1;
            this.t2 = t2;
        }

        public T1 getT1() {
            return t1;
        }

        public T2 getT2() {
            return t2;
        }

        @Override
        public String toString() {
            return getT1() + ", " + getT2();
        }
    }

    public static class CoIterator<T1, T2> implements Iterator<Tuple<T1, T2>> {
        private final Iterator<T1> it1;
        private final Iterator<T2> it2;

        private Tuple<T1, T2> next;

        public CoIterator(Iterator<T1> it1, Iterator<T2> it2) {
            this.it1 = it1;
            this.it2 = it2;
        }

        @Override
        public boolean hasNext() {
            if (next != null) {
                return true;
            }
            T1 t1;
            T2 t2;

            if (it1.hasNext()) {
                t1 = it1.next();
            } else {
                t1 = null;
            }

            if (it2.hasNext()) {
                t2 = it2.next();
            } else if (t1 == null) {
                return false;
            } else {
                t2 = null;
            }

            next = new Tuple<>(t1, t2);
            return true;
        }

        @Override
        public Tuple<T1, T2> next() {
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

    public class SpoId {
        public final int s, p, o, g;
        public final boolean isQuad;

        public SpoId(int s, int p, int o) {
            this.s = s;
            this.p = p;
            this.o = o;
            this.g = -1;
            this.isQuad = false;
        }

        public SpoId(int s, int p, int o, int g) {
            this.s = s;
            this.p = p;
            this.o = o;
            this.g = g;
            this.isQuad = true;
        }

        /**
         * Compute the index in the HDT assuming the order
         * @return index
         */
        public long getIndex() {
            long indexS = s == 0 ? 0 : (s - 1);
            long indexP = p == 0 ? 0 : (p - 1);
            long indexO = o == 0 ? 0 : (o - 1);
            return indexO + objects * (indexP + predicates * indexS);
        }
    }

    public static void printDictionary(HDT hdt) {
        System.out.println("Dictionary");
        Dictionary dict = hdt.getDictionary();

        Map<? extends CharSequence, DictionarySection> sect;

        if (HDTOptionsKeys.DICTIONARY_TYPE_VALUE_MULTI_OBJECTS.equals(dict.getType())) {
            sect = dict.getAllObjects();
        } else {
            Map<String, DictionarySection> sect2 = new TreeMap<>(CharSequenceComparator.getInstance());
            sect2.put("subjects", dict.getSubjects());
            sect2.put("predicates", dict.getPredicates());
            sect2.put("objects", dict.getObjects());
            sect2.put("shareds", dict.getShared());
            sect = sect2;
        }

        sect.forEach((key, sec) -> {
            System.out.println("--- " + key);
            sec.getSortedEntries().forEachRemaining(System.out::println);
        });
    }

    public static void printCoDictionary(HDT hdt, HDT hdt2) {
        System.out.println("Dictionary");
        Dictionary dict = hdt.getDictionary();
        Dictionary dict2 = hdt2.getDictionary();

        Map<? extends CharSequence, DictionarySection> sect1;
        Map<? extends CharSequence, DictionarySection> sect2;

        if (HDTVocabulary.DICTIONARY_TYPE_MULT_SECTION.equals(dict.getType())) {
            sect1 = dict.getAllObjects();
            assertEquals(HDTVocabulary.DICTIONARY_TYPE_MULT_SECTION, dict2.getType());
            sect2 = dict2.getAllObjects();
        } else {
            assertNotEquals(HDTVocabulary.DICTIONARY_TYPE_MULT_SECTION, dict2.getType());
            Map<String, DictionarySection> sect11 = new TreeMap<>(CharSequenceComparator.getInstance());
            sect11.put("subjects", dict.getSubjects());
            sect11.put("predicates", dict.getPredicates());
            sect11.put("objects", dict.getObjects());
            sect11.put("shareds", dict.getShared());
            sect1 = sect11;

            Map<String, DictionarySection> sect21 = new TreeMap<>(CharSequenceComparator.getInstance());
            sect21.put("subjects", dict2.getSubjects());
            sect21.put("predicates", dict2.getPredicates());
            sect21.put("objects", dict2.getObjects());
            sect21.put("shareds", dict2.getShared());
            sect2 = sect21;
        }

        Set<? extends CharSequence> keys = sect1.keySet();
        assertEquals(keys, sect2.keySet());

        keys.forEach(key -> new CoIterator<>(sect1.get(key).getSortedEntries(), sect2.get(key).getSortedEntries())
                .forEachRemaining(System.out::println));
    }

    /**
     * memory hdt
     */
    public final HDT hdt;
    /**
     * the hdt file
     */
    public final File hdtFile;
    /**
     * triples count
     */
    public final int triples;
    /**
     * subject/object shared iris count
     */
    public final int shared;
    /**
     * subjects count
     */
    public final int subjects;
    /**
     * predicates count
     */
    public final int predicates;
    /**
     * objects count
     */
    public final int objects;
    /**
     * graph count
     */
    public final int graphs;
    /**
     * isQuad
     */
    public final boolean isQuad;

    /**
     * create a test hdt with information
     *
     * @param subjects   number of subjects
     * @param predicates number of predicates
     * @param objects    number of objects
     * @param shared     number of shared subjects/objects
     * @param spec       hdt spec
     */
    public HDTTestUtils(File f, int subjects, int predicates, int objects, int shared, HDTOptions spec, boolean buffer) throws IOException {
        this.hdtFile = f;
        this.shared = shared;
        this.subjects = Math.max(shared, subjects);
        this.predicates = predicates;
        this.objects = Math.max(shared, objects);
        this.graphs = -1;
        this.isQuad = false;

        int triples = 0;
        try (
            final TripleWriterHDT wr = new TripleWriterHDT(
                BASE_URI,
                new HDTSpecification(),
                hdtFile.getAbsolutePath(),
                false
            )
        ) {
            for (int i = subjects; i > 0; i--) {
                for (int j = predicates; j > 0; j--) {
                    for (int k = objects; k > 0; k--) {
                        wr.addTriple(spoToTriple(i, j, k));
                        triples++;
                    }
                }
            }
        }
        if (buffer)
            this.hdt = HDTManager.mapHDT(
                hdtFile.getAbsolutePath(),
                null,
                spec
            );
        else
            this.hdt = HDTManager.loadHDT(
                hdtFile.getAbsolutePath(),
                null,
                spec
            );
        assertEquals(
            "HDT count",
            triples,
            hdt.getTriples().getNumberOfElements()
        );
        this.triples = triples;
    }

    /**
     * create a test hdtq with information
     *
     * @param subjects   number of subjects
     * @param predicates number of predicates
     * @param objects    number of objects
     * @param shared     number of shared subjects/objects
     * @param graphs     number of graphs
     * @param spec       hdt spec
     */
    public HDTTestUtils(
        File f,
        int subjects,
        int predicates,
        int objects,
        int shared,
        int graphs,
        HDTOptions spec,
        boolean buffer
    ) throws IOException {
        this.hdtFile = f;
        this.shared = shared;
        this.subjects = Math.max(shared, subjects);
        this.predicates = predicates;
        this.objects = Math.max(shared, objects);
        this.graphs = graphs;
        this.isQuad = true;

        int triples = 0;
        try (
            final TripleWriterHDT wr = new TripleWriterHDT(
                BASE_URI,
                spec,
                hdtFile.getAbsolutePath(),
                false
            )
        ) {
            for (int i = subjects; i > 0; i--) {
                for (int j = predicates; j > 0; j--) {
                    for (int k = objects; k > 0; k--) {
                        for (int l = graphs; l > 0; l--) {
                            wr.addTriple(
                                spoToQuad(i, j, k, l)
                            );
                            triples++;
                        }
                    }
                }
            }
        }
        if (buffer)
            this.hdt = HDTManager.mapHDT(
                hdtFile.getAbsolutePath(),
                null,
                spec
            );
        else
            this.hdt = HDTManager.loadHDT(
                hdtFile.getAbsolutePath(),
                null,
                spec
            );
        assertEquals(
            "HDT count",
            triples / graphs,
            hdt.getTriples().getNumberOfElements()
        );
        this.triples = triples;
    }

    /**
     * convert SpoId into {@link TripleString}
     *
     * @param id spoid
     * @return triplestring
     */
    public TripleString spoToTriple(SpoId id) {
        return spoToTriple(id.s, id.p, id.o);
    }

    /**
     * convert SpoId into {@link TripleString}
     *
     * @param s subject
     * @param p predicate
     * @param o object
     * @return triplestring
     */
    public TripleString spoToTriple(int s, int p, int o) {
        String ss, sp, so;
        if (s == 0) {
            ss = "";
        } else if (s <= shared) {
            ss = BASE_URI + "Sh" + String.format("%05d", s);
        } else {
            ss = BASE_URI + "Su" + String.format("%05d", s - shared);
        }
        if (p == 0) {
            sp = "";
        } else {
            sp = BASE_URI + "Pr" + String.format("%05d", p);
        }
        if (o == 0) {
            so = "";
        } else if (o <= shared) {
            so = BASE_URI + "Sh" + String.format("%05d", o);
        } else {
            so = BASE_URI + "Ob" + String.format("%05d", o - shared);
        }
        return new TripleString(ss, sp, so);
    }

    /**
     * convert SpoId into {@link QuadString}
     *
     * @param s subject
     * @param p predicate
     * @param o object
     * @param g graph
     * @return quadstring
     */
    public QuadString spoToQuad(int s, int p, int o, int g) {
        TripleString tr = spoToTriple(s, p, o);
        String ss = tr.getSubject().toString();
        String sp = tr.getPredicate().toString();
        String so = tr.getObject().toString();
        String sg;
        if (g == 0) {
            sg = "";
        } else {
            sg = BASE_URI + "Gr" + String.format("%05d", g);
        }
        return new QuadString(ss, sp, so, sg);
    }


    /**
     * convert a {@link TripleID} to a {@link SpoId}
     *
     * @param triple hdt triple
     * @return spoid
     */
    public SpoId tripleToSpo(TripleID triple) {
        if (isQuad) {
            return quadToSpo(new QuadString(
                hdt.getDictionary().idToString(triple.getSubject(), TripleComponentRole.SUBJECT),
                hdt.getDictionary().idToString(triple.getPredicate(), TripleComponentRole.PREDICATE),
                hdt.getDictionary().idToString(triple.getObject(), TripleComponentRole.OBJECT),
                hdt.getDictionary().idToString(triple.getGraph(), TripleComponentRole.GRAPH)
            ));
        }
        return tripleToSpo(new TripleString(
                hdt.getDictionary().idToString(triple.getSubject(), TripleComponentRole.SUBJECT),
                hdt.getDictionary().idToString(triple.getPredicate(), TripleComponentRole.PREDICATE),
                hdt.getDictionary().idToString(triple.getObject(), TripleComponentRole.OBJECT)
        ));
    }
    /**
     * convert a {@link TripleString} to a {@link SpoId}
     *
     * @param triple hdt triple
     * @return spoid
     */
    public SpoId tripleToSpo(TripleString triple) {

        int shift = BASE_URI.length();

        String s = triple.getSubject().toString();
        String p = triple.getPredicate().toString();
        String o = triple.getObject().toString();

        int sid = s.isEmpty() ? 0 : Integer.parseInt(s.substring(shift + 2));
        int pid = p.isEmpty() ? 0 : Integer.parseInt(p.substring(shift + 2));
        int oid = o.isEmpty() ? 0 : Integer.parseInt(o.substring(shift + 2));

        if (!s.startsWith("Sh", shift)) sid += shared;

        if (!o.startsWith("Sh", shift)) oid += shared;

        return new SpoId(sid, pid, oid);
    }
    /**
     * convert a {@link QuadString} to a {@link SpoId}
     *
     * @param quad hdt quad
     * @return spoid
     */
    public SpoId quadToSpo(QuadString quad) {

        int shift = BASE_URI.length();

        String s = quad.getSubject().toString();
        String p = quad.getPredicate().toString();
        String o = quad.getObject().toString();
        String g = quad.getGraph().toString();

        int sid = s.isEmpty() ? 0 : Integer.parseInt(s.substring(shift + 2));
        int pid = p.isEmpty() ? 0 : Integer.parseInt(p.substring(shift + 2));
        int oid = o.isEmpty() ? 0 : Integer.parseInt(o.substring(shift + 2));
        int gid = g.isEmpty() ? 0 : Integer.parseInt(g.substring(shift + 2));

        if (!s.startsWith("Sh", shift)) sid += shared;

        if (!o.startsWith("Sh", shift)) oid += shared;

        return new SpoId(sid, pid, oid, gid);
    }

    /**
     * search in the hdt triples, 0 for wildcard
     *
     * @param s subject
     * @param p predicate
     * @param o object
     * @return the iterator
     * @throws NotFoundException if no triples can be found
     */
    public IteratorTripleString searchForSPO(int s, int p, int o) throws NotFoundException {
        TripleString tr = spoToTriple(s, p, o);

        // System.out.println("Search with pattern:" + (s == 0 ? "?" : "S") + (p == 0 ? "?" : "P") + (o == 0 ? "?" : "O"));
        return hdt.search(tr.getSubject(), tr.getPredicate(), tr.getObject());
    }

    /**
     * search in the hdt quads, 0 for wildcard
     *
     * @param s subject
     * @param p predicate
     * @param o object
     * @param g graph
     * @return the iterator
     * @throws NotFoundException if no triples can be found
     */
    public IteratorTripleString searchForSPO(
        int s,
        int p,
        int o,
        int g
    ) throws NotFoundException {
        QuadString tr = spoToQuad(s, p, o, g);
        return null; // TODO: fix this
        // return hdt.search(
        //     tr.getSubject(),
        //     tr.getPredicate(),
        //     tr.getObject(),
        //     tr.getGraph()
        // );
    }

    @Override
    public void close() throws IOException {
        hdt.close();
    }
}
