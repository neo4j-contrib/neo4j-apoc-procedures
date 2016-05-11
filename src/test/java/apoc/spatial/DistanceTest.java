package apoc.spatial;

import apoc.result.DistancePathResult;
import apoc.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.*;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.*;

import static org.junit.Assert.*;
import static apoc.util.TestUtil.testCall;

public class DistanceTest {

    private GraphDatabaseService db;

    private static final String LABEL = "Point";
    private static final String LAT = "latitude";
    private static final String LONG = "longitude";
    private static final String NAME = "name";
    private static final String RELATES = "RELATES";

    @Before
    public void setUp() throws Exception {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        TestUtil.registerProcedure(db, Distance.class);
    }

    @After
    public void tearDown() {
        db.shutdown();
    }

    /* This test fails due to a bug in neo with Path objects returned in scala
    @Test
    public void testCalculateShortestDistanceAndReturnPath() {
        createPoints();
        int i = 0;
        testCall(db, "MATCH (a:Point {name: \"bruges\"}), (b:Point {name: \"dresden\"}) " +
                "MATCH p=(a)-[*]->(b) " +
                "WITH collect(p) as paths " +
                "CALL apoc.spatial.shortestDistancePath(paths) YIELD path RETURN path", (row) -> {
            PathImpl path = (PathImpl) row.get("path");
            List<Node> nodes = new ArrayList<>();
            for (Node node : path.nodes()) {
                nodes.add(node);
            }
            assertEquals(3, nodes.size());
            assertEquals("brussels", nodes.get(1).getProperty(NAME));

        });
    }
    */

    @Test
    public void testSortingPathsOnDistance() {
        Distance distanceProc = new Distance();
        distanceProc.db = db;
        createPoints();
        int i = 0;
        try (Transaction tx = db.beginTx()) {
            List<Path> paths = new ArrayList<>();
            Result result = db.execute("MATCH (a:Point {name:'bruges'}), (b:Point {name:'dresden'}) " +
            "MATCH p=(a)-[*]->(b) RETURN p");
            while (result.hasNext()) {
                Map<String, Object> record = result.next();
                ++i;
                paths.add((Path) record.get("p"));
            }
            int z = 0;
            SortedSet<DistancePathResult> sorted = distanceProc.sortPaths(paths);
            double lastDistance = 0.0;
            Iterator<DistancePathResult> it = sorted.iterator();
            while (it.hasNext()) {
                ++z;
                DistancePathResult d = it.next();
                assertTrue(d.distance > lastDistance);
                lastDistance = d.distance;
            }
            assertEquals(3, z);
            tx.success();
        }
        assertEquals(3, i);
    }

    private void createPoints() {
        try (Transaction tx = db.beginTx()) {
            Node bruges = db.createNode(Label.label(LABEL));
            bruges.setProperty(NAME, "bruges");
            bruges.setProperty(LAT, 51.2605829);
            bruges.setProperty(LONG, 3.0817189);

            Node brussels = db.createNode(Label.label(LABEL));
            brussels.setProperty(NAME, "brussels");
            brussels.setProperty(LAT, 50.854954);
            brussels.setProperty(LONG, 4.3051786);

            Node paris = db.createNode(Label.label(LABEL));
            paris.setProperty(NAME, "paris");
            paris.setProperty(LAT, 48.8588376);
            paris.setProperty(LONG, 2.2773455);

            Node dresden = db.createNode(Label.label(LABEL));
            dresden.setProperty(NAME, "dresden");
            dresden.setProperty(LAT, 51.0767496);
            dresden.setProperty(LONG, 13.6321595);

            bruges.createRelationshipTo(brussels, RelationshipType.withName(RELATES));
            brussels.createRelationshipTo(dresden, RelationshipType.withName(RELATES));
            brussels.createRelationshipTo(paris, RelationshipType.withName(RELATES));
            bruges.createRelationshipTo(paris, RelationshipType.withName(RELATES));
            paris.createRelationshipTo(dresden, RelationshipType.withName(RELATES));

            tx.success();
        }
    }


}
