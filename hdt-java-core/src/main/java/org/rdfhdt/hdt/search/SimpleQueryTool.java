package org.rdfhdt.hdt.search;

import org.rdfhdt.hdt.enums.DictionarySectionRole;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.search.component.HDTComponent;
import org.rdfhdt.hdt.search.component.HDTComponentTriple;
import org.rdfhdt.hdt.search.component.HDTConstant;
import org.rdfhdt.hdt.search.component.HDTVariable;
import org.rdfhdt.hdt.search.component.SimpleHDTComponentTriple;
import org.rdfhdt.hdt.search.component.SimpleHDTConstant;
import org.rdfhdt.hdt.search.component.SimpleHDTVariable;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * simple query tool, use basic join to find results
 */
public class SimpleQueryTool implements HDTQueryTool {
    private static final String AN_PREFIX = "an_" + (new Random().nextLong());
    private static final AtomicLong AN_SUFFIX = new AtomicLong();
    private final HDT hdt;
    private final Map<String, String> prefixes = new HashMap<>();

    public SimpleQueryTool(HDT hdt) {
        this.hdt = hdt;
    }

    @Override
    public HDTQuery createQuery(Collection<HDTComponentTriple> patterns) {
        return new SimpleQueryPattern(this, patterns);
    }

    @Override
    public HDTVariable variable() {
        return new SimpleHDTVariable(hdt, AN_PREFIX + AN_SUFFIX.incrementAndGet());
    }

    @Override
    public HDTVariable variable(String name) {
        return new SimpleHDTVariable(hdt, name);
    }

    @Override
    public HDTConstant constant(CharSequence str) {
        return new SimpleHDTConstant(hdt, str);
    }

    @Override
    public HDTConstant constant(long id, DictionarySectionRole role) {
        return new SimpleHDTConstant(hdt, id, role);
    }

    @Override
    public HDTComponent component(String component) {
        if (component.isEmpty() || component.equals("[]")) {
            return variable();
        }
        if (component.charAt(0) == '?') {
            if (component.length() == 1) {
                return variable();
            } else {
                return variable(component.substring(1));
            }
        }
        if (component.charAt(0) == '"' || component.charAt(0) == '_') {
            // bnode, iri or literal, ignore
            return constant(component);
        }
        if (component.charAt(0) == '<') {
            if (component.charAt(component.length() - 1) != '>') {
                throw new IllegalArgumentException("Bad iri format: " + component);
            }
            return constant(component.substring(1, component.length() - 1));
        }

        int shift = component.indexOf(':');
        if (shift == -1) {
            throw new IllegalArgumentException("Unknown component type: " + component);
        }
        String prefix = component.substring(0, shift);
        String location = prefixes.get(prefix);
        if (location == null) {
            throw new IllegalArgumentException("Unknown prefix: " + prefix + " in " + component);
        }
        return constant(location + component.substring(shift + 1));
    }

    @Override
    public void registerPrefix(String prefix, String location) {
        prefixes.put(prefix, location);
    }

    @Override
    public void unregisterPrefix(String prefix) {
        prefixes.remove(prefix);
    }

    @Override
    public String getPrefix(String prefix) {
        return prefixes.get(prefix);
    }

    @Override
    public Set<String> getPrefixes() {
        return Collections.unmodifiableSet(prefixes.keySet());
    }

    @Override
    public HDTComponentTriple triple(HDTComponent s, HDTComponent p, HDTComponent o) {
        return new SimpleHDTComponentTriple(s, p, o);
    }

    @Override
    public HDT getHDT() {
        return hdt;
    }
}
