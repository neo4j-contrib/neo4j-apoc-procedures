package apoc.warmup;

import apoc.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.Assert.assertEquals;

/**
 * @author Sascha Peukert
 * @since 06.05.16
 */
public class WarmupTest {

    private GraphDatabaseService db;

    @Before
    public void setUp() throws Exception {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        TestUtil.registerProcedure(db,Warmup.class);
    }
    @After
    public void tearDown() {
        db.shutdown();
    }

    @Test
    public void testWarmup() throws Exception {
        // Create enough nodes and relationships to span 2 pages
        db.execute("UNWIND range(1, 300) AS i CREATE (n {foo:1})-[:KNOWS {bar:2}]->(m {foobar:3, array:range(1,100)})").close();
        // Delete all relationships and their nodes, but ones with the minimum and maximum relationship ids, so
        // they still span 2 pages
        db.execute("MATCH ()-[r:KNOWS]->() " +
                   "WITH [min(id(r)), max(id(r))] AS ids " +
                   "MATCH (n)-[r:KNOWS]->(m) " +
                   "WHERE NOT id(r) IN ids " +
                   "DELETE n, m, r").close();

        TestUtil.testCall(db,"CALL apoc.warmup.run()", r ->
        {
            assertEquals(4L, r.get("nodesTotal"));
            assertEquals(2L, r.get("nodePages"));
            assertEquals(546L, r.get("nodesPerPage"));
            assertEquals(2L, r.get("relsTotal"));
            assertEquals(2L, r.get("relPages"));
            assertEquals(240L, r.get("relsPerPage"));
        });
        TestUtil.testCall(db,"CALL apoc.warmup.run(true)", r ->
        {
            assertEquals(true, r.get("propertiesLoaded"));
            assertEquals(900L, r.get("propRecordsTotal"));
            assertEquals(199L, r.get("propsPerPage"));
            assertEquals(5L, r.get("propPages"));
        });
        TestUtil.testCall(db,"CALL apoc.warmup.run(true,true)", r ->
        {
            assertEquals(true, r.get("propertiesLoaded"));
            assertEquals(true, r.get("dynamicPropertiesLoaded"));
            assertEquals(301L, r.get("arrayPropRecordsTotal"));
            assertEquals(64L, r.get("arrayPropsPerPage"));
            assertEquals(5L, r.get("arrayPropPages"));
        });
    }
}
