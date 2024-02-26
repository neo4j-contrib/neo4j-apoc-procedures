package apoc.agg;

import apoc.util.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import static apoc.util.TestUtil.testCall;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class AggregationExtendedTest {

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    @BeforeClass
    public static void setUp() {
        TestUtil.registerProcedure(db, AggregationExtended.class);
        db.executeTransactionally("UNWIND range(0,20) AS id CREATE (:Person {id: 'index' + id})");
        
        db.executeTransactionally("UNWIND [{date: datetime('1999'), other: 5}, " +
                                  "{date: datetime('2000'), other: 10}, " +
                                  "{date: datetime('2000'), other: 15} ] AS prop " +
                                  " CREATE (n:Date) SET n = prop");
    }

    @AfterClass
    public static void tearDown() {
        db.shutdown();
    }

    @Test
    public void testAggRow() {
        testCall(db, "MATCH (n:Person) RETURN apoc.agg.row(n.id, '$curr = \"index10\"') AS row", 
                (row) -> assertEquals(10L, row.get("row")));
    }
    
    @Test
    public void testAggRowWithComplexPredicate() {
        testCall(db, "MATCH (n:Date) RETURN apoc.agg.row(n, '$curr.date <> datetime(\"1999\") AND $curr.other > 11 ') AS row", 
                (row) -> assertEquals(2L, row.get("row")));
    }

    @Test
    public void testPredicateShouldReturnABoolean() {
        try {
            testCall(db, "MATCH (n:Person) RETURN apoc.agg.row(n.id, '1') AS row",
                    (row) -> fail());
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("The predicate query has thrown the following exception: \n" +
                                               "class java.lang.Long cannot be cast to class java.lang.Boolean"));
        }
    }
    
    @Test
    public void testPosition() {
        testCall(db, "MATCH (n:Person) RETURN apoc.agg.position(n.id, 'index10') AS row",
                (row) -> assertEquals(10L, row.get("row")));

    }
}
