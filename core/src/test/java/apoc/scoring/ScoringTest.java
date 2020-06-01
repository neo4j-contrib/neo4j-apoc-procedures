package apoc.scoring;

import apoc.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import static org.junit.Assert.assertEquals;

/**
 * @author mh
 * @since 05.10.16
 */
public class ScoringTest {

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    @BeforeClass
    public static void setUp() throws Exception {
        TestUtil.registerProcedure(db,Scoring.class);
    }

    @Test
    public void existence() throws Exception {
        TestUtil.testCall(db, "RETURN apoc.scoring.existence(10,true) as score", (row) -> assertEquals(10D,row.get("score")));
        TestUtil.testCall(db, "RETURN apoc.scoring.existence(10,false) as score", (row) -> assertEquals(0D,row.get("score")));
    }

    @Test
    public void pareto() throws Exception {
        TestUtil.testResult(db, "UNWIND [0,1,2,8,10,100] as value RETURN value, apoc.scoring.pareto(2,8,10,value) as score", (r) -> {
            assertEquals(0d,r.next().get("score"));
            assertEquals(0d,r.next().get("score"));
            assertEquals(3.3d,(double)r.next().get("score"),0.1d);
            assertEquals(8d,r.next().get("score"));
            assertEquals(8.7,(double)r.next().get("score"),0.1d);
            assertEquals(10d,(double)r.next().get("score"),0.1d);
        });
    }

}
