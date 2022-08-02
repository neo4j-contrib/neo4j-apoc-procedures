package apoc.algo;

import apoc.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.Map;

import static apoc.algo.AlgoUtil.SETUP_GEO;
import static apoc.util.TestUtil.testResult;
import static org.junit.Assert.assertTrue;

public class PathFindingFullTest {

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();


    @BeforeClass
    public static void setUp() throws Exception {
        TestUtil.registerProcedure(db, PathFindingFull.class);
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
        db.executeTransactionally("CREATE (:City {name:'Brescia', lat:45.541553,lon:10.211802})\n" +
                "CREATE (:City {name:'Genova', lat:37.95, lon:12.7})\n" +
                "CREATE (:City {name:'Milano', lat:45.4654219, lon:9.1859243})\n" +
                "CREATE (:City {name:'Firenze', lat:43.833333,lon:11.333333})\n" +
                "CREATE (:City {name:'Frosinone', lat:41.633333,lon:13.316667})\n" +
                "CREATE (:City {name:'Messina', lat:38.1938137,lon:15.5540152})\n" +
                "CREATE (:City {name:'Catanzaro', lat:38.9,lon:16.583333})\n" +
                "CREATE (:City {name:'Cosenza', lat:39.3,lon:16.25})\n" +
                "CREATE (:City {name:'Salerno', lat:40.6824408,lon:14.7680961})\n" +
                "CREATE (:City {name:'Lecce',  lat:40.35481,lon:18.172073})");

        TestUtil.testCall(db, "MATCH (n:City) with collect(n) as nodes " +
                        "CALL apoc.algo.travellingSalesman(nodes, $config) YIELD path, distance RETURN path, distance",
                Map.of("config", Map.of("latitudeProp", "lat", "longitudeProp", "lon")),
                r -> assertTrue((double) r.get("distance") < 3000000));
    }
}
