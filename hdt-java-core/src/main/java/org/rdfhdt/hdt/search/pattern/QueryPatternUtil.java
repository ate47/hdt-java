package org.rdfhdt.hdt.search.pattern;

import org.rdfhdt.hdt.compact.bitmap.Bitmap64Big;
import org.rdfhdt.hdt.compact.bitmap.ModifiableBitmap;
import org.rdfhdt.hdt.search.HDTQuery;
import org.rdfhdt.hdt.search.component.HDTComponentTriple;
import org.rdfhdt.hdt.search.component.HDTVariable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class QueryPatternUtil {
    private static void fillPartition(ModifiableBitmap bitmap, Map<HDTVariable, List<Integer>> varTriple, Set<HDTComponentTriple> partition, List<HDTComponentTriple> list, HDTVariable hvar) {
        varTriple.get(hvar).forEach(tid -> {
            if (bitmap.access(tid)) {
                return; // ignore already parsed elements
            }
            bitmap.set(tid, true);
            HDTComponentTriple triple = list.get(tid);
            partition.add(triple);
            triple.vars()
                    .forEach(shvar -> fillPartition(bitmap, varTriple, partition, list, shvar));
        });
    }

    /**
     * create partitions from the query patterns of a query, each sub graph are the patterns linked by their variables
     *
     * @param query the query
     * @return partitions
     */
    public static List<Set<HDTComponentTriple>> createPartitions(HDTQuery query) {
        List<HDTComponentTriple> list = query.getPatterns();

        if (list.isEmpty()) {
            return List.of();
        }

        if (list.size() == 1) {
            return List.of(Set.of(list.get(0)));
        }

        List<Set<HDTComponentTriple>> partitions = new ArrayList<>();
        Map<HDTVariable, List<Integer>> varTriple = new HashMap<>();

        // map var -> triple id
        for (int i = 0; i < list.size(); i++) {
            for (HDTVariable var : list.get(i).vars()) {
                varTriple
                        .computeIfAbsent(var, k -> new ArrayList<>())
                        .add(i);
            }
        }
        ModifiableBitmap bitmap = Bitmap64Big.memory(list.size());

        for (int i = 0; i < list.size(); i++) {
            if (bitmap.access(i)) {
                continue; // this triple was already parsed
            }
            bitmap.set(i, true);
            Set<HDTComponentTriple> partition = new HashSet<>();
            partitions.add(partition);
            HDTComponentTriple triple = list.get(i);
            partition.add(triple);
            for (HDTVariable hvar : triple.vars()) {
                fillPartition(bitmap, varTriple, partition, list, hvar);
            }
        }

        return partitions;
    }
}
