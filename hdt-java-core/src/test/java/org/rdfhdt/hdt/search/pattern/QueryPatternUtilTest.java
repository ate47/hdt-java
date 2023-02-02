package org.rdfhdt.hdt.search.pattern;

import org.junit.Test;
import org.rdfhdt.hdt.compact.bitmap.Bitmap64Big;
import org.rdfhdt.hdt.compact.bitmap.ModifiableBitmap;
import org.rdfhdt.hdt.search.AbstractQueryTest;
import org.rdfhdt.hdt.search.component.HDTComponentTriple;
import org.rdfhdt.hdt.search.component.HDTVariable;

import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class QueryPatternUtilTest extends AbstractQueryTest {

    private void partitionTest(List<Set<HDTComponentTriple>> excepted, List<Set<HDTComponentTriple>> actual) {
        assertEquals("the size isn't the same", excepted.size(), actual.size());
        ModifiableBitmap bitmap = Bitmap64Big.memory(excepted.size());

        exceptedLoop:
        for (Set<HDTComponentTriple> set : actual) {
            for (int i = 0; i < excepted.size(); i++) {
                if (bitmap.access(i)) {
                    continue;
                }
                if (excepted.get(i).equals(set)) {
                    bitmap.set(i, true);
                    continue exceptedLoop;
                }
            }
            fail("Actual set isn't in the excepted list: " + set);
        }
    }

    @Test
    public void partitionTest() {
        HDTVariable v1 = MOCK_FACTORY.variable();
        HDTVariable v2 = MOCK_FACTORY.variable();

        HDTComponentTriple t1 = MOCK_FACTORY.triple(
                MOCK_FACTORY.constant("test"),
                v1,
                MOCK_FACTORY.constant("1")
        );
        HDTComponentTriple t2 = MOCK_FACTORY.triple(
                MOCK_FACTORY.constant("test2"),
                MOCK_FACTORY.constant("p"),
                v1
        );
        HDTComponentTriple t3 = MOCK_FACTORY.triple(
                MOCK_FACTORY.constant("test"),
                v2,
                MOCK_FACTORY.constant("1")
        );

        List<Set<HDTComponentTriple>> actual = QueryPatternUtil.createPartitions(MOCK_FACTORY.createQuery(t1, t2, t3));

        List<Set<HDTComponentTriple>> excepted = List.of(
                Set.of(t1, t2),
                Set.of(t3)
        );

        partitionTest(excepted, actual);
    }
    @Test
    public void partitionEmptyTest() {

        HDTComponentTriple t1 = MOCK_FACTORY.triple(
                MOCK_FACTORY.constant("test"),
                MOCK_FACTORY.constant("p"),
                MOCK_FACTORY.constant("1")
        );
        HDTComponentTriple t2 = MOCK_FACTORY.triple(
                MOCK_FACTORY.constant("test2"),
                MOCK_FACTORY.constant("p"),
                MOCK_FACTORY.constant("3")
        );
        HDTComponentTriple t3 = MOCK_FACTORY.triple(
                MOCK_FACTORY.constant("test"),
                MOCK_FACTORY.constant("p"),
                MOCK_FACTORY.constant("1")
        );

        List<Set<HDTComponentTriple>> actual = QueryPatternUtil.createPartitions(MOCK_FACTORY.createQuery(t1, t2, t3));

        List<Set<HDTComponentTriple>> excepted = List.of(
                Set.of(t1),
                Set.of(t2),
                Set.of(t3)
        );

        partitionTest(excepted, actual);
    }

    @Test
    public void partition2Test() {
        HDTVariable v1 = MOCK_FACTORY.variable();
        HDTVariable v2 = MOCK_FACTORY.variable();
        HDTVariable v3 = MOCK_FACTORY.variable();

        HDTComponentTriple t1 = MOCK_FACTORY.triple(
                MOCK_FACTORY.constant("test"),
                v1,
                MOCK_FACTORY.constant("1")
        );
        HDTComponentTriple t2 = MOCK_FACTORY.triple(
                v2,
                MOCK_FACTORY.constant("p"),
                v1
        );
        HDTComponentTriple t3 = MOCK_FACTORY.triple(
                MOCK_FACTORY.constant("test"),
                v2,
                MOCK_FACTORY.constant("1")
        );
        HDTComponentTriple t4 = MOCK_FACTORY.triple(
                MOCK_FACTORY.constant("test"),
                v3,
                MOCK_FACTORY.constant("2")
        );

        List<Set<HDTComponentTriple>> actual = QueryPatternUtil
                .createPartitions(MOCK_FACTORY.createQuery(
                        t1, t2, t3, t4
                ));

        List<Set<HDTComponentTriple>> excepted = List.of(
                Set.of(t1, t2, t3),
                Set.of(t4)
        );

        partitionTest(excepted, actual);
    }

    @Test
    public void partitionNamedTest() {
        HDTVariable v1 = MOCK_FACTORY.variable("v1");
        HDTVariable v2 = MOCK_FACTORY.variable("v2");
        HDTVariable v3 = MOCK_FACTORY.variable("v3");

        HDTComponentTriple t1 = MOCK_FACTORY.triple(
                MOCK_FACTORY.constant("test"),
                v1,
                MOCK_FACTORY.constant("1")
        );
        HDTComponentTriple t2 = MOCK_FACTORY.triple(
                v2,
                MOCK_FACTORY.constant("p"),
                v1
        );
        HDTComponentTriple t3 = MOCK_FACTORY.triple(
                MOCK_FACTORY.constant("test"),
                v2,
                MOCK_FACTORY.constant("1")
        );
        HDTComponentTriple t4 = MOCK_FACTORY.triple(
                MOCK_FACTORY.constant("test"),
                v3,
                MOCK_FACTORY.constant("2")
        );

        List<Set<HDTComponentTriple>> actual = QueryPatternUtil
                .createPartitions(MOCK_FACTORY.createQuery(
                        t1, t2, t3, t4
                ));

        List<Set<HDTComponentTriple>> excepted = List.of(
                Set.of(t1, t2, t3),
                Set.of(t4)
        );

        partitionTest(excepted, actual);
    }
    @Test
    public void partitionNamedCopyTest() {

        HDTComponentTriple t1 = MOCK_FACTORY.triple(
                MOCK_FACTORY.constant("test"),
                MOCK_FACTORY.variable("v1"),
                MOCK_FACTORY.constant("1")
        );
        HDTComponentTriple t2 = MOCK_FACTORY.triple(
                MOCK_FACTORY.variable("v2"),
                MOCK_FACTORY.constant("p"),
                MOCK_FACTORY.variable("v1")
        );
        HDTComponentTriple t3 = MOCK_FACTORY.triple(
                MOCK_FACTORY.constant("test"),
                MOCK_FACTORY.variable("v2"),
                MOCK_FACTORY.constant("1")
        );
        HDTComponentTriple t4 = MOCK_FACTORY.triple(
                MOCK_FACTORY.constant("test"),
                MOCK_FACTORY.variable("v3"),
                MOCK_FACTORY.constant("2")
        );

        List<Set<HDTComponentTriple>> actual = QueryPatternUtil
                .createPartitions(MOCK_FACTORY.createQuery(
                        t1, t2, t3, t4
                ));

        List<Set<HDTComponentTriple>> excepted = List.of(
                Set.of(t1, t2, t3),
                Set.of(t4)
        );

        partitionTest(excepted, actual);
    }
}