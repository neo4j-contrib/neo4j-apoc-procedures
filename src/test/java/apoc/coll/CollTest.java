package apoc.coll;

import apoc.util.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.neo4j.helpers.collection.Iterables.asSet;

public class CollTest {

    private static GraphDatabaseService db;
    @BeforeClass public static void setUp() throws Exception {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        TestUtil.registerProcedure(db,Coll.class);
    }
    @AfterClass public static void tearDown() {
        db.shutdown();
    }

    @Test public void testZip() throws Exception {
        testCall(db, "RETURN apoc.coll.zip([1,2,3],[4,5]) as value",
                (row) -> {
                    Object value = row.get("value");
                    List<List<Long>> expected = asList(asList(1L, 4L), asList(2L, 5L), asList(3L, null));
                    assertEquals(expected, value);
                });
    }
    @Test public void testPairs() throws Exception {
        testCall(db, "RETURN apoc.coll.pairs([1,2,3]) as value",
                (row) -> assertEquals(asList(asList(1L,2L),asList(2L,3L),asList(3L,null)), row.get("value")));
    }
    @Test public void testPairsMin() throws Exception {
        testCall(db, "RETURN apoc.coll.pairsMin([1,2,3]) as value",
                (row) -> assertEquals(asList(asList(1L,2L),asList(2L,3L)), row.get("value")));
    }
    @Test public void testPairsMinListResult() throws Exception {
        testCall(db, "RETURN apoc.coll.pairsMin([1,2,3])[0][0] as result",
                (row) -> assertEquals(1L, row.get("result")));
    }
    @Test public void testToSet() throws Exception {
        testCall(db, "RETURN apoc.coll.toSet([1,2,1,3]) as value",
                (row) -> assertEquals(asList(1L,2L,3L), row.get("value")));
    }
    @Ignore
    @Test public void testSum() throws Exception {
        testCall(db, "RETURN `apoc.coll.sum`([1,2,3]) as value",
                (row) -> assertEquals(6D, row.get("value")));
    }
    @Ignore
    @Test public void testAvg() throws Exception {
        testCall(db, "RETURN `apoc.coll.avg`([1.4,2,3.2]) as value",
                (row) -> assertEquals(2.2D, (double)row.get("value"),0.1));
    }
    @Ignore
    @Test public void testMin() throws Exception {
        testCall(db, "RETURN `apoc.coll.min`([1,2,3]) as value",
                (row) -> assertEquals(1L, row.get("value")));
    }
    @Ignore
    @Test public void testMax() throws Exception {
        testCall(db, "RETURN `apoc.coll.max`([1,2,3]) as value",
                (row) -> assertEquals(3L, row.get("value")));
    }
    @Test public void testPartition() throws Exception {
        testResult(db, "CALL apoc.coll.partition([1,2,3,4,5],2)",
                (result) -> {
                    Map<String, Object> row = result.next();
                    assertEquals(asList(1L,2L), row.get("value"));
                    row = result.next();
                    assertEquals(asList(3L,4L), row.get("value"));
                    row = result.next();
                    assertEquals(asList(5L), row.get("value"));
                    assertFalse(result.hasNext());
                });
    }

    @Test public void testSumLongs() throws Exception {
        testCall(db, "RETURN apoc.coll.sumLongs([1,2,3]) AS value",
                (row) -> assertEquals(6L, row.get("value")));
    }
    @Test public void testSort() throws Exception {
        testCall(db, "RETURN apoc.coll.sort([3,2,1]) as value",
                (row) -> assertEquals(asList(1L,2L,3L), row.get("value")));
    }

    @Test public void testIN() throws Exception {
        testCall(db, "RETURN apoc.coll.contains([1,2,3],1) AS value",
                (res) -> assertEquals(true, res.get("value")));
    }
    @Test public void testIndexOf() throws Exception {
        testCall(db, "RETURN apoc.coll.indexOf([1,2,3],1) AS value", r -> assertEquals(0L, r.get("value")));
        testCall(db, "RETURN apoc.coll.indexOf([1,2,3],2) AS value", r -> assertEquals(1L, r.get("value")));
        testCall(db, "RETURN apoc.coll.indexOf([1,2,3],3) AS value", r -> assertEquals(2L, r.get("value")));
        testCall(db, "RETURN apoc.coll.indexOf([1,2,3],4) AS value", r -> assertEquals(-1L, r.get("value")));
        testCall(db, "RETURN apoc.coll.indexOf([1,2,3],0) AS value", r -> assertEquals(-1L, r.get("value")));
        testCall(db, "RETURN apoc.coll.indexOf([1,2,3],null) AS value", r -> assertEquals(-1L, r.get("value")));
    }

    @Test public void testSplit() throws Exception {
        testResult(db, "CALL apoc.coll.split([1,2,3,2,4,5],2)", r -> {
            assertEquals(asList(1L), r.next().get("value"));
            assertEquals(asList(3L), r.next().get("value"));
            assertEquals(asList(4L,5L), r.next().get("value"));
            assertFalse(r.hasNext());
        });
        testResult(db, "CALL apoc.coll.split([1,2,3],2)", r -> {
            assertEquals(asList(1L), r.next().get("value"));
            assertEquals(asList(3L), r.next().get("value"));
            assertFalse(r.hasNext());
        });
        testResult(db, "CALL apoc.coll.split([1,2,3],1)", r -> {
            assertEquals(asList(2L,3L), r.next().get("value"));
            assertFalse(r.hasNext());
        });
        testResult(db, "CALL apoc.coll.split([1,2,3],3)", r -> {
            assertEquals(asList(1L,2L), r.next().get("value"));
            assertFalse(r.hasNext());
        });
        testResult(db, "CALL apoc.coll.split([1,2,3],4)", r -> {
            assertEquals(asList(1L,2L,3L), r.next().get("value"));
            assertFalse(r.hasNext());
        });
    }

    @Test public void testContainsAll() throws Exception {
        testCall(db, "RETURN apoc.coll.containsAll([1,2,3],[1,2]) AS value", (res) -> assertEquals(true, res.get("value")));
        testCall(db, "RETURN apoc.coll.containsAll([1,2,3],[1,4]) AS value", (res) -> assertEquals(false, res.get("value")));
        testCall(db, "RETURN apoc.coll.containsAll([1,2,3],[]) AS value", (res) -> assertEquals(true, res.get("value")));
        testCall(db, "RETURN apoc.coll.containsAll([1,2,3],[1]) AS value", (res) -> assertEquals(true, res.get("value")));
        testCall(db, "RETURN apoc.coll.containsAll([1,2,3],[1,2,3,4]) AS value", (res) -> assertEquals(false, res.get("value")));
    }
    @Test public void testContainsAllSorted() throws Exception {
        testCall(db, "RETURN apoc.coll.containsAllSorted([1,2,3],[1,2]) AS value", (res) -> assertEquals(true, res.get("value")));
        testCall(db, "RETURN apoc.coll.containsAllSorted([1,2,3],[1,4]) AS value", (res) -> assertEquals(false, res.get("value")));
        testCall(db, "RETURN apoc.coll.containsAllSorted([1,2,3],[]) AS value", (res) -> assertEquals(true, res.get("value")));
        testCall(db, "RETURN apoc.coll.containsAllSorted([1,2,3],[1]) AS value", (res) -> assertEquals(true, res.get("value")));
        testCall(db, "RETURN apoc.coll.containsAllSorted([1,2,3],[1,2,3,4]) AS value", (res) -> assertEquals(false, res.get("value")));
    }

    @Test public void testIN2() throws Exception {
        int elements = 1_000_000;
        ArrayList<Long> list = new ArrayList<>(elements);
        for (long i = 0; i< elements; i++) {
            list.add(i);
        }
        Map<String, Object> params = new HashMap<>();
        params.put("list", list);
        params.put("value", list.get(list.size()-1));
        long start = System.currentTimeMillis();
        testCall(db, "RETURN apoc.coll.contains({list},{value}) AS value", params,
                (res) -> assertEquals(true, res.get("value")));
        System.out.printf("contains test on %d elements took %d ms%n", elements, System.currentTimeMillis() - start);
    }
    @Test public void testContainsSorted() throws Exception {
        int elements = 1_000_000;
        ArrayList<Long> list = new ArrayList<>(elements);
        for (long i = 0; i< elements; i++) {
            list.add(i);
        }
        Map<String, Object> params = new HashMap<>();
        params.put("list", list);
        params.put("value", list.get(list.size()/2));
        long start = System.currentTimeMillis();
        testCall(db, "RETURN apoc.coll.containsSorted({list},{value}) AS value", params,
                (res) -> assertEquals(true, res.get("value")));
        System.out.printf("contains sorted test on %d elements took %d ms%n", elements, System.currentTimeMillis() - start);
    }

    @Test public void testSortNodes() throws Exception {
        testCall(db,
            "CREATE (n {name:'foo'}),(m {name:'bar'}) WITH n,m RETURN apoc.coll.sortNodes([n,m], 'name') AS value",
            (row) -> {
                List<Node> nodes = (List<Node>) row.get("value");
                assertEquals("bar", nodes.get(0).getProperty("name"));
                assertEquals("foo", nodes.get(1).getProperty("name"));
            });
    }

    @Test
    public void testSetOperations() throws Exception {
        testCall(db,"RETURN apoc.coll.union([1,2],[3,2]) AS value", r -> assertEquals(asSet(asList(1L,2L,3L)),asSet((Iterable)r.get("value"))));
        testCall(db,"RETURN apoc.coll.intersection([1,2],[3,2]) AS value", r -> assertEquals(asSet(asList(2L)),asSet((Iterable)r.get("value"))));
        testCall(db,"RETURN apoc.coll.disjunction([1,2],[3,2]) AS value", r -> assertEquals(asSet(asList(1L,3L)),asSet((Iterable)r.get("value"))));
        testCall(db,"RETURN apoc.coll.subtract([1,2],[3,2]) AS value", r -> assertEquals(asSet(asList(1L)),asSet((Iterable)r.get("value"))));
        testCall(db,"RETURN apoc.coll.unionAll([1,2],[3,2]) AS value", r -> assertEquals(asList(1L,2L,3L,2L),r.get("value")));
        testCall(db,"RETURN apoc.coll.removeAll([1,2],[3,2]) AS value", r -> assertEquals(asList(1L),r.get("value")));

    }
}
