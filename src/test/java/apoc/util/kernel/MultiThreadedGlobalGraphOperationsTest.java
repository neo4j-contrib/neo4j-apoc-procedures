package apoc.util.kernel;

import org.junit.*;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static apoc.util.kernel.MultiThreadedGlobalGraphOperations.GlobalOperationsTypes.NODES;
import static apoc.util.kernel.MultiThreadedGlobalGraphOperations.GlobalOperationsTypes.RELATIONSHIPS;
import static org.junit.Assert.*;
import static apoc.util.kernel.MultiThreadedGlobalGraphOperations.*;

public class MultiThreadedGlobalGraphOperationsTest {

    private static GraphDatabaseAPI db;

    @BeforeClass
    public static void beforeClass() throws Exception {
        db = (GraphDatabaseAPI) new TestGraphDatabaseFactory().newImpermanentDatabase();
        createData();
    }

    private static void createData() {
        db.execute("UNWIND range(1,1000) as x MERGE (s{id:x}) MERGE (e{id:x+1}) merge (s)-[:REL{id:x}]->(e)");
    }

    @AfterClass
    public static void afterClass() throws Exception {
        if (db!=null) db.shutdown();
    }

    @Test
    public void shouldforAllNodesWork() {
        AtomicInteger counter = new AtomicInteger();
        BatchJobResult result = forAllNodes(db, Executors.newFixedThreadPool(4), 10,
                (ktx,nodeCursor) -> counter.incrementAndGet());
        assertEquals(1001, counter.get());
        final long highestIdInUse = getHighestIdInUseForStore(db.getDependencyResolver(), NODES);
        assertEquals(highestIdInUse / 10, result.getBatches());
        assertEquals( 1001, result.getSucceeded());

        long countOfNodes = Iterators.single(db.execute("match (n) return count(n) as count").columnAs("count"));

        assertEquals( highestIdInUse - countOfNodes, result.getMissing());
        assertEquals( 0, result.getFailures());
    }

    @Test
    public void shouldforAllRelationshipsWork() {
        AtomicInteger counter = new AtomicInteger();
        BatchJobResult result = forAllRelationships(db, Executors.newFixedThreadPool(4), 10,
                (ktx, relationshipScanCursor) -> counter.incrementAndGet());
        assertEquals(1000, counter.get());
        final long highestIdInUse = getHighestIdInUseForStore(db.getDependencyResolver(), RELATIONSHIPS);
        assertEquals(highestIdInUse / 10, result.getBatches());
        assertEquals( 1000, result.getSucceeded());
        assertEquals( 0, result.getMissing());
        assertEquals( 0, result.getFailures());
    }
}
