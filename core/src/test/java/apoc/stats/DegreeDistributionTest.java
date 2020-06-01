package apoc.stats;

import apoc.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * @author mh
 * @since 07.08.17
 */
public class DegreeDistributionTest {

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();


    @BeforeClass
    public static void setUp() throws Exception {
        TestUtil.registerProcedure(db, DegreeDistribution.class);
        db.executeTransactionally("UNWIND range(1,10) as rels CREATE (f:Foo) WITH * UNWIND range(1,rels) as r CREATE (f)-[:BAR]->(f)");
    }

    @Test
    public void degrees() throws Exception {
        TestUtil.testCall(db, "CALL apoc.stats.degrees()", row -> {
            assertEquals(null,row.get("type"));
            assertEquals("BOTH",row.get("direction"));
            assertEquals(55L,row.get("total"));
            assertEquals(10L,row.get("max"));
            assertEquals(1L,row.get("min"));
            assertEquals(5.5d,row.get("mean"));
            assertEquals(10L,row.get("p99"));
            assertEquals(5L,row.get("p50"));
        });
        TestUtil.testCall(db, "CALL apoc.stats.degrees('BAR>')", row -> {
            assertEquals("BAR",row.get("type"));
            assertEquals("OUTGOING",row.get("direction"));
            assertEquals(55L,row.get("total"));
            assertEquals(10L,row.get("max"));
            assertEquals(1L,row.get("min"));
            assertEquals(5.5d,row.get("mean"));
            assertEquals(10L,row.get("p99"));
            assertEquals(5L,row.get("p50"));
        });
    }
    @Test
    public void allDegrees() throws Exception {
        TestUtil.testResult(db, "CALL apoc.stats.degrees('*')", result -> {
            Map<String, Object> row = result.next();
            assertEquals("BAR",row.get("type"));
            assertEquals("OUTGOING",row.get("direction"));
            assertEquals(55L,row.get("total"));
            assertEquals(10L,row.get("max"));
            assertEquals(1L,row.get("min"));
            assertEquals(5.5d,row.get("mean"));
            assertEquals(10L,row.get("p99"));
            assertEquals(5L,row.get("p50"));
            row = result.next();
            assertEquals("BAR",row.get("type"));
            assertEquals("INCOMING",row.get("direction"));
            assertEquals(55L,row.get("total"));
            assertEquals(10L,row.get("max"));
            assertEquals(1L,row.get("min"));
            assertEquals(5.5d,row.get("mean"));
            assertEquals(10L,row.get("p99"));
            assertEquals(5L,row.get("p50"));
            assertFalse(result.hasNext());
        });
    }

}
