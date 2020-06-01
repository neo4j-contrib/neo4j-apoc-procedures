package apoc.algo;

import apoc.util.TestUtil;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Result;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.List;
import java.util.Map;

import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static apoc.util.Util.map;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class PathFindingTest {

    private static final String SETUP_MISSING_PROPERTY = "CREATE " +
            "(a:Loc{name:'A'}), " +
            "(b:Loc{name:'B'}), " +
            "(c:Loc{name:'C'}), " +
            "(d:Loc{name:'D'}), " +
            "(a)-[:ROAD {d:100}]->(d), " +
            "(a)-[:RAIL {d:5}]->(d), " +
            "(a)-[:ROAD {d:'10'}]->(b), " +
            "(b)-[:ROAD {d:20}]->(c), " +
            "(c)-[:ROAD]->(d), " +
            "(a)-[:ROAD {d:20}]->(c) ";
    private static final String SETUP_SIMPLE = "CREATE " +
            "(a:Loc{name:'A'}), " +
            "(b:Loc{name:'B'}), " +
            "(c:Loc{name:'C'}), " +
            "(d:Loc{name:'D'}), " +
            "(a)-[:ROAD {d:100}]->(d), " +
            "(a)-[:RAIL {d:5}]->(d), " +
            "(a)-[:ROAD {d:10}]->(b), " +
            "(b)-[:ROAD {d:20}]->(c), " +
            "(c)-[:ROAD {d:30}]->(d), " +
            "(a)-[:ROAD {d:20}]->(c) ";
    private static final String SETUP_GEO = "CREATE (b:City {name:'Berlin',lat:52.52464,lon:13.40514})\n" +
            "CREATE (m:City {name:'München',lat:48.1374,lon:11.5755})\n" +
            "CREATE (f:City {name:'Frankfurt',lat:50.1167,lon:8.68333})\n" +
            "CREATE (h:City {name:'Hamburg',lat:53.554423,lon:9.994583})\n" +
            "CREATE (b)-[:DIRECT {dist:255.64*1000}]->(h)\n" +
            "CREATE (b)-[:DIRECT {dist:504.47*1000}]->(m)\n" +
            "CREATE (b)-[:DIRECT {dist:424.12*1000}]->(f)\n" +
            "CREATE (f)-[:DIRECT {dist:304.28*1000}]->(m)\n" +
            "CREATE (f)-[:DIRECT {dist:393.15*1000}]->(h)";

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();


    @Before
   	public void setUp() throws Exception {
   		TestUtil.registerProcedure(db, PathFinding.class);
   	}

    @Test
    public void testAStar() {
        db.executeTransactionally(SETUP_GEO);
        testResult(db,
                "MATCH (from:City {name:'München'}), (to:City {name:'Hamburg'}) " +
                        "CALL apoc.algo.aStar(from, to, 'DIRECT', 'dist', 'lat', 'lon') yield path, weight " +
                        "RETURN path, weight" ,
                r -> assertAStarResult(r)
        );
    }

    @Test
    public void testAStarConfig() {
        db.executeTransactionally(SETUP_GEO);
        testResult(db,
                "MATCH (from:City {name:'München'}), (to:City {name:'Hamburg'}) " +
                        "CALL apoc.algo.aStarConfig(from, to, 'DIRECT', {weight:'dist',y:'lat', x:'lon',default:100}) yield path, weight " +
                        "RETURN path, weight" ,
                r -> assertAStarResult(r)
        );
    }

    private void assertAStarResult(Result r) {
        assertEquals(true, r.hasNext());
        Map<String, Object> row = r.next();
        assertEquals(697, ((Number)row.get("weight")).intValue()/1000) ;
        Path path = (Path) row.get("path");
        assertEquals(2, path.length()) ; // 3nodes, 2 rels
        List<Node> nodes = Iterables.asList(path.nodes());
        assertEquals("München", nodes.get(0).getProperty("name")) ;
        assertEquals("Frankfurt", nodes.get(1).getProperty("name")) ;
        assertEquals("Hamburg", nodes.get(2).getProperty("name")) ;
        assertEquals(false,r.hasNext());
    }

    @Test
    public void testDijkstra() {
        db.executeTransactionally(SETUP_SIMPLE);
        testCall(db,
            "MATCH (from:Loc{name:'A'}), (to:Loc{name:'D'}) " +
            "CALL apoc.algo.dijkstra(from, to, 'ROAD>', 'd') yield path, weight " +
            "RETURN path, weight" ,
            row ->  {
                assertEquals(50.0, row.get("weight")) ;
                assertEquals(2, ((Path)(row.get("path"))).length()) ; // 3nodes, 2 rels
            }
        );
        testCall(db,
            "MATCH (from:Loc{name:'A'}), (to:Loc{name:'D'}) " +
            "CALL apoc.algo.dijkstra(from, to, '', 'd') yield path, weight " +
            "RETURN path, weight" ,
            row ->  {
                assertEquals(5.0, row.get("weight")) ;
                assertEquals(1, ((Path)(row.get("path"))).length()) ; // 2nodes, 1 rels
            }
        );
    }

    @Test
    public void testDijkstraWithDefaultWeight() {
        db.executeTransactionally(SETUP_MISSING_PROPERTY);
        testCall(db,
                "MATCH (from:Loc{name:'A'}), (to:Loc{name:'D'}) " +
                        "CALL apoc.algo.dijkstraWithDefaultWeight(from, to, 'ROAD>', 'd', 10.5) yield path, weight " +
                        "RETURN path, weight",
                row -> {
                    assertEquals(30.5, row.get("weight"));
                    assertEquals(2, ((Path) (row.get("path"))).length()); // 3nodes, 2 rels
                }
        );
    }

    @Test
    public void testDijkstraMultipleShortest() {
        db.executeTransactionally(SETUP_SIMPLE);
        testResult(db,
                "MATCH (from:Loc{name:'A'}), (to:Loc{name:'D'}) " +
                        "CALL apoc.algo.dijkstra(from, to, 'ROAD>', 'd', 99999, 3) yield path, weight " +
                        "RETURN path, weight",
                result -> {
                    List<Map<String, Object>> records = Iterators.asList(result);
                    assertThat(
                            map(records, map -> map.get("weight")),
                            contains(50.0, 60.0, 100.0)
                    );

                    assertThat(
                            map(records, map -> ((Path) map.get("path")).length()),
                            contains(2, 3, 1)
                    );
                }
        );
    }

    @Test
    public void testAllSimplePaths() {
        db.executeTransactionally(SETUP_MISSING_PROPERTY);
        testResult(db,
                "MATCH (from:Loc{name:'A'}), (to:Loc{name:'D'}) " +
                        "CALL apoc.algo.allSimplePaths(from, to, 'ROAD>', 3) yield path " +
                        "RETURN path ORDER BY length(path)",
                res -> {
                    Path path;
                    path = (Path) res.next().get("path");
                    assertEquals(1, path.length());
                    path = (Path) res.next().get("path");
                    assertEquals(2, path.length());
                    path = (Path) res.next().get("path");
                    assertEquals(3, path.length());
                    assertEquals(false, res.hasNext());
                }
        );
    }
    @Test
    public void testAllSimplePathResults() {
        db.executeTransactionally(SETUP_MISSING_PROPERTY);
        testResult(db,
                "MATCH (from:Loc{name:'A'}), (to:Loc{name:'D'}) " +
                        "CALL apoc.algo.allSimplePaths(from, to, 'ROAD>', 3) yield path " +
                        "RETURN nodes(path) as nodes ORDER BY length(path)",
                res -> {
                    List nodes;
                    nodes = (List) res.next().get("nodes");
                    assertEquals(2, nodes.size());
                    nodes = (List) res.next().get("nodes");
                    assertEquals(3, nodes.size());
                    nodes = (List) res.next().get("nodes");
                    assertEquals(4, nodes.size());
                    assertEquals(false, res.hasNext());
                }
        );
    }
}
