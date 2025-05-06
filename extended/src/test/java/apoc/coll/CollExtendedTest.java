package apoc.coll;

import apoc.util.TestUtil;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.Values;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;

import static apoc.util.TestUtil.testCall;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

public class CollExtendedTest {

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();
    
    @BeforeClass
    public static void setUp() throws Exception {
        TestUtil.registerProcedure(db, CollExtended.class);
    }

    @Test
    public void testAvgDuration() {
        final List<DurationValue> list = List.of(
                DurationValue.parse("P2DT4H1S"), DurationValue.parse("PT1H1S"), DurationValue.parse("PT1H6S"), DurationValue.parse("PT1H5S"));
        
        // get duration from Neo4j aggregation AvgFunction
        final DurationValue expected = TestUtil.singleResultFirstColumn(db, "UNWIND $list AS dur RETURN avg(dur) AS value",
                Map.of("list", list));
        
        // same duration values as above
        testCall(db, "WITH $list AS dur RETURN apoc.coll.avgDuration(dur) AS value",
                Map.of("list", list),
                (row) -> assertEquals(expected, row.get("value")));
    }

    @Test
    public void testAvgDurationNullOrEmpty() {
        testCall(db, "WITH [] AS dur " +
                        "RETURN apoc.coll.avgDuration(dur) AS value",
                (row) -> assertNull(row.get("value")));
        
        testCall(db, "WITH null AS dur " +
                        "RETURN apoc.coll.avgDuration(dur) AS value",
                (row) -> assertNull(row.get("value")));
        
    }

    @Test
    public void testAvgDurationWrongType() {
        final String queryIntType = "WITH [1,2,3] AS dur " +
                "RETURN apoc.coll.avgDuration(dur)";
        testWrongType(queryIntType);

        final String queryMixedType = "WITH [duration('P2DT4H1S'), duration('PT1H6S'), 1] AS dur " +
                "RETURN apoc.coll.avgDuration(dur)";
        testWrongType(queryMixedType);
    }

    private void testWrongType(String query) {
        try {
            testCall(db, query, row -> fail("should fail due to Wrong argument type"));
        } catch (RuntimeException e) {
            String expected = "Can't coerce `Long(1)` to Duration";
            MatcherAssert.assertThat(e.getMessage(), Matchers.containsString(expected));
        }
    }

    @Test
    public void testFillObject() {
        testCall(db, "RETURN apoc.coll.fillObject('abc',2) as value",
                (row) -> assertEquals(List.of("abc", "abc"), row.get("value"))
        );

        testCall(db, "RETURN apoc.coll.fillObject(5,3) as value",
                (row) -> assertEquals(List.of(5L,5L,5L), row.get("value"))
        );

        testCall(db, "WITH {a:1, b:[2,3]} AS item RETURN apoc.coll.fillObject(item, 3) as value",
                (row) -> {
                    Map<String, Object> item = Map.of("a", 1L, "b", List.of(2L, 3L));
                    assertEquals(List.of(item, item, item), row.get("value"));
                }
        );

        testCall(db, "WITH [1,2,3] AS item RETURN apoc.coll.fillObject(item, 3) as value",
                (row) -> {
                    List<Long> item = List.of(1L, 2L, 3L);
                    assertEquals(List.of(item, item, item), row.get("value"));
                }
        );

        testCall(db, "CREATE (node:Node {a: 1}) RETURN node, apoc.coll.fillObject(node, 2) as value",
                (row) -> {
                    Object node = row.get("node");
                    assertEquals(List.of(node, node), row.get("value"));
                }
        );

        testCall(db, "RETURN apoc.coll.fillObject() as value",
                (row) -> assertEquals(List.of(), row.get("value"))
        );
    }

    @Test
    public void testSetNodePropertiesUsingFillObject() {
        testCall(db, """
                CREATE (n:FillNode)
                SET n.empty = apoc.coll.fillObject(),
                    n.int = apoc.coll.fillObject(5, 2),
                    n.double = apoc.coll.fillObject(5.2, 2),
                    n.date = apoc.coll.fillObject(date('2020'), 2),
                    n.point = apoc.coll.fillObject(point({x: 1, y: 1}), 3)
                RETURN n""", r -> {
            final Map<String, Object> props = ((Node) r.get("n")).getAllProperties();
            
            assertArrayEquals(new String[0], (Object[]) props.get("empty"));
            assertArrayEquals(new long[] {5L, 5L}, (long[]) props.get("int"));
            assertArrayEquals(new double[] {5.2D, 5.2D}, (double[]) props.get("double"), 0.1D);

            LocalDate localDate = LocalDate.of(2020, 1, 1);
            final Object[] expectedDate = { localDate, localDate };
            assertArrayEquals(expectedDate, (Object[]) props.get("date"));

            PointValue pointValue = Values.pointValue(CoordinateReferenceSystem.CARTESIAN, 1, 1);
            final Object[] expectedPoint = { pointValue, pointValue, pointValue };
            assertArrayEquals(expectedPoint, (Object[]) props.get("point"));
        });

        db.executeTransactionally("MATCH (n:FillNode) DELETE n");
    }
}
