package apoc.algo;

import apoc.util.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;


public class PathFindingExtendedTest {

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    @BeforeClass
    public static void setUp() throws Exception {
        TestUtil.registerProcedure(db, PathFindingExtended.class);
    }

    @AfterClass
    public static void tearDown() {
        db.shutdown();
    }

    @Test
    public void testAStarWithPoint() {
//        db.executeTransactionally(SETUP_GEO);
//        testResult(db,
//                "MATCH (from:City {name:'München'}), (to:City {name:'Hamburg'}) " +
//                        "CALL apoc.algo.aStarWithPoint(from, to, 'DIRECT', 'dist', 'coords') yield path, weight " +
//                        "RETURN path, weight" ,
//                AlgoUtil::assertAStarResult
//        );
    }
}
