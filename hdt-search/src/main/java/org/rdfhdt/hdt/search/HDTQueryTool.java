package org.rdfhdt.hdt.search;

import org.rdfhdt.hdt.enums.DictionarySectionRole;
import org.rdfhdt.hdt.enums.TripleComponentRole;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.search.component.HDTComponent;
import org.rdfhdt.hdt.search.component.HDTComponentTriple;
import org.rdfhdt.hdt.search.component.HDTConstant;
import org.rdfhdt.hdt.search.component.HDTVariable;

import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
 * HDT allowing the query with multiple patterns
 *
 * @author Antoine Willerval
 */
public interface HDTQueryTool {

    /**
     * Create a query from multiple triple patterns
     *
     * @param patterns the patterns
     * @return query
     */
    default HDTQuery createQuery(HDTComponentTriple... patterns) {
        return createQuery(List.of(patterns));
    }

    /**
     * Create a query from multiple triple patterns
     *
     * @param patterns the patterns
     * @return query
     */
    HDTQuery createQuery(Collection<HDTComponentTriple> patterns);

    /**
     * create a variable with no name
     *
     * @return HDTVariable
     */
    HDTVariable variable();

    /**
     * create a named variable
     *
     * @param name name
     * @return HDTVariable
     */
    HDTVariable variable(String name);

    /**
     * create a constant from a CharSequence in the HDT
     *
     * @param str CharSequence
     * @return HdtConstant
     */
    HDTConstant constant(CharSequence str);

    /**
     * create a constant from an id in the HDT
     *
     * @param id   id
     * @param role role
     * @return HdtConstant
     */
    HDTConstant constant(long id, DictionarySectionRole role);

    /**
     * create a component from a description
     * <p>
     * "", "?" is equivalent to {@link #variable()}
     * <p>
     * "?name" is equivalent to {@link #variable(String)} using "name"
     * <p>
     * literals are the same as in SPARQL
     *
     * @param component component description
     * @return component
     */
    HDTComponent component(String component);

    /**
     * create triple from 3 components, same as using {@link #component(String)} on the 3 components
     *
     * @param s subject
     * @param p predicate
     * @param o object
     * @return HDTComponentTriple
     */
    default HDTComponentTriple triple(String s, String p, String o) {
        return triple(component(s), component(p), component(o));
    }

    /**
     * register a prefix
     *
     * @param prefix   prefix
     * @param location location
     */
    void registerPrefix(String prefix, String location);

    /**
     * unregister a prefix
     *
     * @param prefix prefix
     */
    void unregisterPrefix(String prefix);

    /**
     * get a prefix location
     *
     * @param prefix prefix
     * @return prefix location
     */
    String getPrefix(String prefix);

    /**
     * @return the prefixes
     */
    Set<String> getPrefixes();

    /**
     * create triple from components
     *
     * @param s subject
     * @param p predicate
     * @param o object
     * @return HDTComponentTriple
     */
    HDTComponentTriple triple(HDTComponent s, HDTComponent p, HDTComponent o);

    /**
     * create a constant from an id in the HDT
     *
     * @param id   id
     * @param role role
     * @return HdtConstant
     */
    default HDTConstant constant(long id, TripleComponentRole role) {
        return constant(id, role.asDictionarySectionRole());
    }


    /**
     * @return the HDT linked with this tool
     */
    HDT getHDT();
}
