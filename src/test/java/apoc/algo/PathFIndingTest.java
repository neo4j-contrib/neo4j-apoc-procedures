package apoc.algo;

import apoc.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.List;
import java.util.Map;

import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class PathFindingTest {

    private GraphDatabaseService db;
    public static final String SETUP = "CREATE (b:City {name:'Berlin',lat:52.52464,lon:13.40514})\n" +
            "CREATE (m:City {name:'München',lat:48.1374,lon:11.5755})\n" +
            "CREATE (f:City {name:'Frankfurt',lat:50.1167,lon:8.68333})\n" +
            "CREATE (h:City {name:'Hamburg',lat:53.554423,lon:9.994583})\n" +
            "CREATE (b)-[:DIRECT {dist:255.64*1000}]->(h)\n" +
            "CREATE (b)-[:DIRECT {dist:504.47*1000}]->(m)\n" +
            "CREATE (b)-[:DIRECT {dist:424.12*1000}]->(f)\n" +
            "CREATE (f)-[:DIRECT {dist:304.28*1000}]->(m)\n" +
            "CREATE (f)-[:DIRECT {dist:393.15*1000}]->(h)";

    @Before
   	public void setUp() throws Exception {
   		db = new TestGraphDatabaseFactory().newImpermanentDatabase();
   		TestUtil.registerProcedure(db, PathFinding.class);
   	}

   	@After
   	public void tearDown() {
   		db.shutdown();
   	}

    @Test
    public void testAStar() throws Exception {
        db.execute(SETUP).close();
        testResult(db,
                "MATCH (from:City {name:'München'}), (to:City {name:'Hamburg'}) " +
                        "CALL apoc.algo.aStar(from, to, 'DIRECT', 'dist', 'lat', 'lon') yield path, weight " +
                        "RETURN path, weight" ,
                r ->  {
                    assertEquals(true, r.hasNext());
                    Map<String, Object> row = r.next();
                    assertEquals(697, ((Number)row.get("weight")).intValue()/1000) ;
                    List path = (List)row.get("path");
                    assertEquals(5, path.size()) ; // 3nodes, 2 rels
                    assertEquals("München", ((Node)path.get(0)).getProperty("name")) ;
                    assertEquals("Frankfurt", ((Node)path.get(2)).getProperty("name")) ;
                    assertEquals("Hamburg", ((Node)path.get(4)).getProperty("name")) ;

                    assertEquals(false,r.hasNext());
                }
        );
    }
    @Test
    public void testAStarConfig() throws Exception {
        db.execute(SETUP).close();
        testResult(db,
                "MATCH (from:City {name:'München'}), (to:City {name:'Hamburg'}) " +
                        "CALL apoc.algo.aStarConfig(from, to, 'DIRECT', {weight:'dist',y:'lat', x:'lon',default:100}) yield path, weight " +
                        "RETURN path, weight" ,
                r ->  {
                    assertEquals(true, r.hasNext());
                    Map<String, Object> row = r.next();
                    assertEquals(697, ((Number)row.get("weight")).intValue()/1000) ;
                    List path = (List)row.get("path");
                    assertEquals(5, path.size()) ; // 3nodes, 2 rels
                    assertEquals("München", ((Node)path.get(0)).getProperty("name")) ;
                    assertEquals("Frankfurt", ((Node)path.get(2)).getProperty("name")) ;
                    assertEquals("Hamburg", ((Node)path.get(4)).getProperty("name")) ;

                    assertEquals(false,r.hasNext());
                }
        );
    }

    @Test
    public void testDijkstra() {
        db.execute("CREATE " +
                "(a:Loc{name:'A'}), " +
                "(b:Loc{name:'B'}), " +
                "(c:Loc{name:'C'}), " +
                "(d:Loc{name:'D'}), " +
                "(a)-[:ROAD {d:100}]->(d), " +
                "(a)-[:RAIL {d:5}]->(d), " +
                "(a)-[:ROAD {d:10}]->(b), " +
                "(b)-[:ROAD {d:20}]->(c), " +
                "(c)-[:ROAD {d:30}]->(d), " +
                "(a)-[:ROAD {d:20}]->(c) ").close();
        testCall(db,
            "MATCH (from:Loc{name:'A'}), (to:Loc{name:'D'}) " +
            "CALL apoc.algo.dijkstra(from, to, 'ROAD>', 'd') yield path, weight " +
            "RETURN path, weight" ,
            row ->  {
                assertEquals(50.0, row.get("weight")) ;
                assertEquals(5, ((List)(row.get("path"))).size()) ; // 3nodes, 2 rels
            }
        );
        testCall(db,
            "MATCH (from:Loc{name:'A'}), (to:Loc{name:'D'}) " +
            "CALL apoc.algo.dijkstra(from, to, '', 'd') yield path, weight " +
            "RETURN path, weight" ,
            row ->  {
                assertEquals(5.0, row.get("weight")) ;
                assertEquals(3, ((List)(row.get("path"))).size()) ; // 2nodes, 1 rels
            }
        );
    }

    @Test
    public void testDijkstraWithDefaultWeight() {
        db.execute("CREATE " +
                "(a:Loc{name:'A'}), " +
                "(b:Loc{name:'B'}), " +
                "(c:Loc{name:'C'}), " +
                "(d:Loc{name:'D'}), " +
                "(a)-[:ROAD {d:100}]->(d), " +
                "(a)-[:RAIL {d:5}]->(d), " +
                "(a)-[:ROAD {d:10}]->(b), " +
                "(b)-[:ROAD {d:20}]->(c), " +
                "(c)-[:ROAD]->(d), " +
                "(a)-[:ROAD {d:20}]->(c) ").close();
        testCall(db,
                "MATCH (from:Loc{name:'A'}), (to:Loc{name:'D'}) " +
                        "CALL apoc.algo.dijkstraWithDefaultWeight(from, to, 'ROAD>', 'd', 10.5) yield path, weight " +
                        "RETURN path, weight",
                row -> {
                    assertEquals(30.5, row.get("weight"));
                    assertEquals(5, ((List) (row.get("path"))).size()); // 3nodes, 2 rels
                }
        );
    }

    @Test
    public void testAllSimplePaths() {
        db.execute("CREATE " +
                "(a:Loc{name:'A'}), " +
                "(b:Loc{name:'B'}), " +
                "(c:Loc{name:'C'}), " +
                "(d:Loc{name:'D'}), " +
                "(a)-[:ROAD {d:100}]->(d), " +
                "(a)-[:RAIL {d:5}]->(d), " +
                "(a)-[:ROAD {d:10}]->(b), " +
                "(b)-[:ROAD {d:20}]->(c), " +
                "(c)-[:ROAD]->(d), " +
                "(a)-[:ROAD {d:20}]->(c) ").close();
        testResult(db,
                "MATCH (from:Loc{name:'A'}), (to:Loc{name:'D'}) " +
                        "CALL apoc.algo.allSimplePaths(from, to, 'ROAD>', 3) yield path " +
                        "RETURN path ORDER BY length(path)",
                res -> {
                    List path;
                    path = (List) res.next().get("path");
                    assertEquals(3, path.size());
                    path = (List) res.next().get("path");
                    assertEquals(5, path.size());
                    path = (List) res.next().get("path");
                    assertEquals(7, path.size());
                    assertEquals(false, res.hasNext());
                }
        );
    }
}
