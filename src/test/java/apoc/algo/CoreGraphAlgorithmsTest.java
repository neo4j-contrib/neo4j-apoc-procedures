package apoc.algo;

import org.junit.*;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.Arrays;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * @author mh
 * @since 12.06.16
 */
public class CoreGraphAlgorithmsTest {

    private static GraphDatabaseAPI db;
    private Transaction tx;
    private Statement stmt;
    private static int idA, idB, idC, idD;

    @BeforeClass
    public static void beforeClass() throws Exception {
        db = (GraphDatabaseAPI) new TestGraphDatabaseFactory().newImpermanentDatabase();
        createData();
    }

    @Before
    public void setUp() throws Exception {
        tx = db.beginTx();
        stmt = db.getDependencyResolver().resolveDependency(ThreadToStatementContextBridge.class).get();
    }

    private static void createData() {
        Result result = db.execute("CREATE (a:A {id:'a'})-[:X]->(b:A {id:'b'})-[:Y]->(c:C {id:'c'}), (d:D {id:'d'}) return id(a),id(b),id(c),id(d)");
        Map<String, Object> row = result.next();
        idA = (int)(long)row.get("id(a)");
        idB = (int)(long)row.get("id(b)");
        idC = (int)(long)row.get("id(c)");
        idD = (int)(long)row.get("id(d)");
        result.close();
    }

    @After
    public void tearDown() throws Exception {
        if (tx!=null) {
            tx.failure();
            tx.close();
        }
    }
    @AfterClass
    public static void afterClass() throws Exception {
        if (db!=null) db.shutdown();
    }

    @Test
    public void testInitAll() throws Exception {
        CoreGraphAlgorithms algos = new CoreGraphAlgorithms(stmt).init();
        assertEquals(4,algos.getNodeCount());
        assertEquals(2,algos.getRelCount());
        int[] offsets = algos.getNodeRelOffsets();
        assertEquals(0,offsets[idA]);
        assertEquals(1,offsets[idB]);
        assertEquals(2,offsets[idC]);
        assertEquals(2,offsets[idD]);
        int[] rels = algos.getRels();
        assertEquals(idB,rels[0]);
        assertEquals(idC,rels[1]);
    }

    @Test
    public void testInitLabel() throws Exception {
        CoreGraphAlgorithms algos = new CoreGraphAlgorithms(stmt).init("A");
        assertEquals(2,algos.getNodeCount());
        assertEquals(2,algos.getRelCount());
        int[] degrees = algos.getNodeRelOffsets();
        assertEquals(0,degrees[idA]);
        assertEquals(1,degrees[idB]);
        int[] rels = algos.getRels();
        assertEquals(idB,rels[0]);
        assertEquals(idC,rels[1]);
    }
    @Test
    public void testInitLabelRel() throws Exception {
        CoreGraphAlgorithms algos = new CoreGraphAlgorithms(stmt).init("A","X");
        assertEquals(2,algos.getNodeCount());
        assertEquals(1,algos.getRelCount());
        int[] degrees = algos.getNodeRelOffsets();
        assertEquals(0,degrees[idA]);
        assertEquals(1,degrees[idB]);
        int[] rels = algos.getRels();
        assertEquals(idB,rels[0]);
    }

    @Test
    public void pageRank() throws Exception {
        CoreGraphAlgorithms algos = new CoreGraphAlgorithms(stmt).init();
        float[] rank = algos.pageRank(2);
        System.out.println("rank = " + Arrays.toString(rank));
        assertEquals(0.85f,rank[idA],0f);
        assertEquals(0.9775f,rank[idB],0f);
        assertEquals(0.9775f,rank[idC],0f);
        assertEquals(0,rank[idD],0f);

    }

    @Test
    public void labelPropagation() throws Exception {
        CoreGraphAlgorithms algos = new CoreGraphAlgorithms(stmt).init();
        int[] labels = algos.labelPropagation();
        assertEquals(idA,labels[idA]);
        assertEquals(idA,labels[idB]);
        assertEquals(idA,labels[idC]);
        assertEquals(idD,labels[idD]);
    }

    @Test
    public void unionFind() throws Exception {
        CoreGraphAlgorithms algos = new CoreGraphAlgorithms(stmt).init();
        int[] labels = algos.unionFind();
        assertEquals(idA,labels[idA]);
        assertEquals(idA,labels[idB]);
        assertEquals(idA,labels[idC]);
        assertEquals(idD,labels[idD]);
    }

    @Test
    public void testLoadDegreesOutgoing() throws Exception {
        CoreGraphAlgorithms algos = new CoreGraphAlgorithms(stmt).init();
        int[] degrees = algos.loadDegrees(null, Direction.OUTGOING);
        assertEquals(1,degrees[idA]);
        assertEquals(1,degrees[idB]);
        assertEquals(0,degrees[idC]);
        assertEquals(0,degrees[idD]);
    }
    @Test
    public void testLoadDegreesBoth() throws Exception {
        CoreGraphAlgorithms algos = new CoreGraphAlgorithms(stmt).init();
        int[] degrees = algos.loadDegrees(null, Direction.BOTH);
        assertEquals(1,degrees[idA]);
        assertEquals(2,degrees[idB]);
        assertEquals(1,degrees[idC]);
        assertEquals(0,degrees[idD]);
    }

    @Test
    public void testLoadDegreesOutgoingType() throws Exception {
        CoreGraphAlgorithms algos = new CoreGraphAlgorithms(stmt).init();
        int[] degrees = algos.loadDegrees("X", Direction.OUTGOING);
        assertEquals(1,degrees[idA]);
        assertEquals(0,degrees[idB]);
        assertEquals(0,degrees[idC]);
        assertEquals(0,degrees[idD]);
    }
    @Test
    public void testLoadDegreesIncoming() throws Exception {
        CoreGraphAlgorithms algos = new CoreGraphAlgorithms(stmt).init();
        int[] degrees = algos.loadDegrees(null, Direction.INCOMING);
        assertEquals(0,degrees[idA]);
        assertEquals(1,degrees[idB]);
        assertEquals(1,degrees[idC]);
        assertEquals(0,degrees[idD]);
    }
    @Test
    public void testLoadDegreesIncomingType() throws Exception {
        CoreGraphAlgorithms algos = new CoreGraphAlgorithms(stmt).init();
        int[] degrees = algos.loadDegrees("Y", Direction.INCOMING);
        assertEquals(0,degrees[idA]);
        assertEquals(0,degrees[idB]);
        assertEquals(1,degrees[idC]);
        assertEquals(0,degrees[idD]);
    }

}
