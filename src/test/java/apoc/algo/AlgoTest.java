package apoc.algo;

import apoc.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.List;

import static apoc.util.TestUtil.testCall;
import static org.junit.Assert.assertEquals;

public class AlgoTest {

   	private GraphDatabaseService db;

   	@Before
   	public void setUp() throws Exception {
   		db = new TestGraphDatabaseFactory().newImpermanentDatabase();
   		TestUtil.registerProcedure(db, Algo.class);
   	}

   	@After
   	public void tearDown() {
   		db.shutdown();
   	}

    @Test
    public void testDijekstra() {
        db.execute("CREATE " +
                "(a:Loc{name:'A'}), " +
                "(b:Loc{name:'B'}), " +
                "(c:Loc{name:'C'}), " +
                "(d:Loc{name:'D'}), " +
                "(a)-[:ROAD {d:100}]->(d), " +
                "(a)-[:ROAD {d:10}]->(b), " +
                "(b)-[:ROAD {d:20}]->(c), " +
                "(c)-[:ROAD {d:30}]->(d), " +
                "(a)-[:ROAD {d:20}]->(c) ");
        testCall(db,
            "MATCH (from:Loc{name:'A'}), (to:Loc{name:'D'}) " +
            "CALL apoc.algo.dijkstra(from, to, 'd') yield path as path, weight as weight " +
            "RETURN path, weight" ,
            row ->  {
                assertEquals(50.0, row.get("weight")) ;
                assertEquals(5, ((List)(row.get("path"))).size()) ; // 3nodes, 2 rels
            }
        );
    }

    @Test
    public void testDijekstraWithDefaultWeight() {
        db.execute("CREATE " +
                "(a:Loc{name:'A'}), " +
                "(b:Loc{name:'B'}), " +
                "(c:Loc{name:'C'}), " +
                "(d:Loc{name:'D'}), " +
                "(a)-[:ROAD {d:100}]->(d), " +
                "(a)-[:ROAD {d:10}]->(b), " +
                "(b)-[:ROAD {d:20}]->(c), " +
                "(c)-[:ROAD]->(d), " +
                "(a)-[:ROAD {d:20}]->(c) ");
        testCall(db,
                "MATCH (from:Loc{name:'A'}), (to:Loc{name:'D'}) " +
                        "CALL apoc.algo.dijkstraWithDefaultWeight(from, to, 'd', 10.5) yield path as path, weight as weight " +
                        "RETURN path, weight",
                row -> {
                    assertEquals(30.5, row.get("weight"));
                    assertEquals(5, ((List) (row.get("path"))).size()); // 3nodes, 2 rels
                }
        );
    }
}
