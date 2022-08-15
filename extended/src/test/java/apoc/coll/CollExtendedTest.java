package apoc.coll;

import apoc.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;
import org.neo4j.values.storable.DurationValue;

import java.util.List;
import java.util.Map;

import static apoc.util.TestUtil.testCall;
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
            assertEquals("Wrong argument type: Can't coerce `Long(1)` to Duration", e.getMessage());
        }
    }
}
