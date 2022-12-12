package apoc.algo;

import apoc.util.TestUtil;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.Map;

import static apoc.algo.AlgoUtil.SETUP_GEO;
import static apoc.algo.TravellingSalesman.Config.POINT_PROP_KEY;
import static apoc.util.TestUtil.testResult;
import static org.junit.Assert.assertTrue;

public class PathFindingFullTest {

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();


    @BeforeClass
    public static void setUp() throws Exception {
        TestUtil.registerProcedure(db, PathFindingFull.class);
    }
    
    @After
    public void after() {
        db.executeTransactionally("MATCH (n) DETACH DELETE n");
    } 
    
    @Test
    public void testAStarWithPoint() {
        db.executeTransactionally(SETUP_GEO);
        testResult(db,
                "MATCH (from:City {name:'München'}), (to:City {name:'Hamburg'}) " +
                        "CALL apoc.algo.aStarWithPoint(from, to, 'DIRECT', 'dist', 'coords') yield path, weight " +
                        "RETURN path, weight" ,
                AlgoUtil::assertAStarResult
        );
    }

    @Test
    public void testTravellingSalesman() {
        db.executeTransactionally("CREATE (:City {name:'Brescia', place: point({ latitude:45.541553,longitude:10.211802}) }),\n" +
                "(:City {name:'Genova', place: point({latitude:37.95, longitude:12.7}) }), \n" +
                "(:City {name:'Milano', place: point({latitude:45.4654219, longitude:9.1859243}) }), \n" +
                "(:City {name:'Firenze', place: point({latitude:43.833333, longitude:11.333333}) }), \n" +
                "(:City {name:'Frosinone', place: point({latitude:41.633333, longitude:13.316667}) }), \n" +
                "(:City {name:'Messina', place: point({latitude:38.1938137, longitude:15.5540152}) }), \n" +
                "(:City {name:'Catanzaro', place: point({latitude:38.9, longitude:16.583333}) }), \n" +
                "(:City {name:'Cosenza', place: point({latitude:39.3, longitude:16.25}) }), \n" +
                "(:City {name:'Salerno', place: point({latitude:40.6824408, longitude:14.7680961}) }), \n" +
                "(:City {name:'Lecce', place: point({latitude:40.35481, longitude:18.172073}) })");

        TestUtil.testCall(db, "MATCH (n:City) with collect(n) as nodes " +
                        "CALL apoc.algo.travellingSalesman(nodes) YIELD path, distance RETURN path, distance",
                PathFindingFullTest::travelingSalesmanAssertions);
    }

    @Test
    public void testTravellingSalesmanCustomPointPropAndXYCoords() {
        db.executeTransactionally("CREATE (:City {name:'Brescia', location: point({ y:45.541553,x:10.211802}) }),\n" + 
                "(:City {name:'Genova', location: point({y:37.95, x:12.7}) }), \n" +
                "(:City {name:'Milano', location: point({y:45.4654219, x:9.1859243}) }), \n" +
                "(:City {name:'Firenze', location: point({y:43.833333, x:11.333333}) }), \n" +
                "(:City {name:'Frosinone', location: point({y:41.633333, x:13.316667}) }), \n" +
                "(:City {name:'Messina', location: point({y:38.1938137, x:15.5540152}) }), \n" +
                "(:City {name:'Catanzaro', location: point({y:38.9, x:16.583333}) }), \n" +
                "(:City {name:'Cosenza', location: point({y:39.3, x:16.25}) }), \n" +
                "(:City {name:'Salerno', location: point({y:40.6824408, x:14.7680961}) }), \n" +
                "(:City {name:'Lecce', location: point({y:40.35481, x:18.172073}) })");
        
        TestUtil.testCall(db, "MATCH (n:City) with collect(n) as nodes " + 
                        "CALL apoc.algo.travellingSalesman(nodes, $config) YIELD path, distance RETURN path, distance", 
                Map.of("config", Map.of(POINT_PROP_KEY, "location")),
                PathFindingFullTest::travelingSalesmanAssertions);
    }

    private static void travelingSalesmanAssertions(Map<String, Object> r) {
        final double distance = (double) r.get("distance");
        final boolean assertCondition = distance > 2500000 && distance < 3000000;
        assertTrue("The actual distance is " + distance, assertCondition);
    }
}
