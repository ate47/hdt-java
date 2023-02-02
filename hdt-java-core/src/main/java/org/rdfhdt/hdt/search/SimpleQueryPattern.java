package org.rdfhdt.hdt.search;

import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.search.component.HDTComponent;
import org.rdfhdt.hdt.search.component.HDTComponentTriple;
import org.rdfhdt.hdt.search.component.HDTVariable;
import org.rdfhdt.hdt.search.query.NestedJoinQueryIterator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class SimpleQueryPattern implements HDTQuery {
    private final HDTQueryTool tool;
    private final List<HDTComponentTriple> patterns;
    private final Map<String, HDTVariable> variableMap;
    private long timeout = 0;

    public SimpleQueryPattern(HDTQueryTool tool, Collection<HDTComponentTriple> patterns) {
        this.tool = tool;
        // use hashset to remove duplicated elements
        this.variableMap = new HashMap<>();
        this.patterns = new ArrayList<>(patterns);
        for (HDTComponentTriple pattern : patterns) {
            Objects.requireNonNull(pattern, "a pattern is null!");

            // register the components
            registerVariable(pattern.getSubject());
            registerVariable(pattern.getPredicate());
            registerVariable(pattern.getObject());
        }
    }

    private void registerVariable(HDTComponent component) {
        if (!component.isVariable()) {
            return;
        }
        HDTVariable variable = component.asVariable();
        variableMap.put(variable.getName(), variable);
    }


    @Override
    public Iterator<HDTQueryResult> query() {
        return new NestedJoinQueryIterator(tool.getHDT(), this, 0);
    }

    @Override
    public Set<String> getVariableNames() {
        return variableMap.keySet();
    }

    public HDT getHdt() {
        return tool.getHDT();
    }

    @Override
    public List<HDTComponentTriple> getPatterns() {
        return patterns;
    }

    @Override
    public void setTimeout(long millis) {
        timeout = Math.max(0, millis);
    }

    @Override
    public long getTimeout() {
        return timeout;
    }

    @Override
    public String toString() {
        // convert the query into a SPARQL query
        return getPatterns().stream()
                        .map(o -> "  " + o + "\n")
                        .collect(Collectors.joining("", "SELECT "
                                + getVariableNames().stream()
                                .map(v -> "?" + v)
                                .collect(Collectors.joining(" "))
                                + " {\n", "}"));
    }
}
