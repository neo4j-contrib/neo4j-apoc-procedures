package apoc.neighbors;

import apoc.util.TestUtil;
import org.junit.*;
import org.neo4j.graphdb.*;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class NeighborsTest {

    private GraphDatabaseService db;

    @Before
    public void setUp() throws Exception {
        db = TestUtil.apocGraphDatabaseBuilder().newGraphDatabase();
        TestUtil.registerProcedure(db, Neighbors.class);
        db.execute("CREATE (a:First), " +
                "(b:Neighbor), " +
                "(c:Neighbor), " +
                "(d:Neighbor), " +
                "(a)-[:KNOWS]->(b), " +
                "(b)-[:KNOWS]->(a), " +
                "(b)-[:KNOWS]->(c), " +
                "(c)-[:KNOWS]->(d) ").close();
    }

    @After
    public void tearDown() {
        db.shutdown();
    }

    @Test
    public void getNeighbors2Hops() {
        TestUtil.testCall(db, "MATCH (n:First) WITH n " +
                        "CALL apoc.neighbors(n,'KNOWS>', 2) YIELD node AS neighbor " +
                        "RETURN COLLECT(neighbor) AS neighbors",
                (row) -> {
                    List<Node> neighbors = (List<Node>) row.get("neighbors");
                    assertEquals(2, neighbors.size());
                });
    }

    @Test
    public void getNeighbors3Hops() {
        TestUtil.testCall(db, "MATCH (n:First) WITH n " +
                        "CALL apoc.neighbors(n,'KNOWS>', 3) YIELD node AS neighbor " +
                        "RETURN COLLECT(neighbor) AS neighbors",
                (row) -> {
                    List<Node> neighbors = (List<Node>) row.get("neighbors");
                    assertEquals(3, neighbors.size());
                });
    }

    @Test
    public void getNeighborsCount2Hops() {
        TestUtil.testCall(db, "MATCH (n:First) WITH n " +
                        "CALL apoc.neighbors.count(n,'KNOWS>', 2) YIELD value AS number " +
                        "RETURN number",
                (row) -> assertEquals(2L, row.get("number")));
    }

    @Test
    public void getNeighborsCount3Hops() {
        TestUtil.testCall(db, "MATCH (n:First) WITH n " +
                        "CALL apoc.neighbors.count(n,'KNOWS>', 3) YIELD value AS number " +
                        "RETURN number",
                (row) -> assertEquals(3L, row.get("number")));
    }

    @Test
    public void getNeighborsByHop2Hops() {
        TestUtil.testCall(db, "MATCH (n:First) WITH n " +
                        "CALL apoc.neighbors.byhop(n,'KNOWS>', 2) YIELD nodes AS neighbor " +
                        "RETURN COLLECT(neighbor) AS neighbors",
                (row) -> {
                    List<List<Node>> neighbors = (List<List<Node>>) row.get("neighbors");
                    assertEquals(2, neighbors.size());
                });
    }

    @Test
    public void getNeighborsByHop3Hops() {
        TestUtil.testCall(db, "MATCH (n:First) WITH n " +
                        "CALL apoc.neighbors.byhop(n,'KNOWS>', 3) YIELD nodes AS neighbor " +
                        "RETURN COLLECT(neighbor) AS neighbors",
                (row) -> {
                    List<List<Node>> neighbors = (List<List<Node>>) row.get("neighbors");
                    assertEquals(3, neighbors.size());
                });
    }

    @Test
    public void getNeighborsByHopCount2Hops() {
        TestUtil.testCall(db, "MATCH (n:First) WITH n " +
                        "CALL apoc.neighbors.count.byhop(n,'KNOWS>', 2) YIELD value AS numbers " +
                        "RETURN numbers",
                (row) -> {
                    List<Long> numbers = (List<Long>) row.get("numbers");
                    assertEquals(2, numbers.size());
                });
    }

    @Test
    public void getNeighborsByHopCount3Hops() {
        TestUtil.testCall(db, "MATCH (n:First) WITH n " +
                        "CALL apoc.neighbors.count.byhop(n,'KNOWS>', 3) YIELD value AS numbers " +
                        "RETURN numbers",
                (row) -> {
                    List<Long> numbers = (List<Long>) row.get("numbers");
                    assertEquals(3, numbers.size());
                });
    }
}
