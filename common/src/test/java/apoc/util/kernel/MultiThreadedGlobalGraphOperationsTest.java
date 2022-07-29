package apoc.util.kernel;

import apoc.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import static apoc.util.kernel.MultiThreadedGlobalGraphOperations.BatchJobResult;
import static apoc.util.kernel.MultiThreadedGlobalGraphOperations.forAllNodes;
import static apoc.util.kernel.MultiThreadedGlobalGraphOperations.forAllRelationships;
import static org.junit.Assert.assertEquals;

public class MultiThreadedGlobalGraphOperationsTest {

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    @BeforeClass
    public static void beforeClass() {
        createData();
    }

    private static void createData() {
        db.executeTransactionally("UNWIND range(1,1000) as x MERGE (s{id:x}) MERGE (e{id:x+1}) merge (s)-[:REL{id:x}]->(e)");
    }

    @Test
    public void shouldforAllNodesWork() {
        AtomicInteger counter = new AtomicInteger();
        BatchJobResult result = forAllNodes(db, Executors.newFixedThreadPool(4), 10,
                (ktx,nodeCursor) -> counter.incrementAndGet() );
        assertEquals(1001, counter.get());
        assertEquals(1001, result.getSucceeded());

        long countOfNodes = TestUtil.singleResultFirstColumn(db, "match (n) return count(n) as count");

        assertEquals(0, result.getFailures());
    }

    @Test
    public void shouldforAllRelationshipsWork() {
        AtomicInteger counter = new AtomicInteger();
        BatchJobResult result = forAllRelationships(db, Executors.newFixedThreadPool(4), 10,
                (ktx, relationshipScanCursor) -> counter.incrementAndGet());
        assertEquals(1000, counter.get());
        assertEquals(1000, result.getSucceeded());
        assertEquals(0, result.getFailures());
    }
}
