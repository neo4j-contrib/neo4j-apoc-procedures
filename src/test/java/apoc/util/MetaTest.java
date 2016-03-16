package apoc.util;

import apoc.create.Create;
import apoc.meta.Meta;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.*;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.List;

import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static org.junit.Assert.assertEquals;

public class MetaTest {

    private GraphDatabaseService db;

    @Before public void setUp() throws Exception {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        TestUtil.registerProcedure(db,Meta.class); }

    @After public void tearDown() {
        db.shutdown();
    }

    @Test public void testMetaGraph() throws Exception {
        db.execute("CREATE (:Actor)-[:ACTED_IN]->(:Movie) ").close();
        testCall(db, "CALL apoc.meta.graph",
                (row) -> {
                    List<Node> nodes = (List<Node>) row.get("nodes");
                    Node n1 = nodes.get(0);
                    assertEquals("Meta",n1.getLabels().iterator().next().name());
                    assertEquals(1,n1.getProperty("count"));
                    assertEquals("Actor",n1.getProperty("name"));
                    Node n2 = nodes.get(1);
                    assertEquals("Meta",n2.getLabels().iterator().next().name());
                    assertEquals("Movie",n2.getProperty("name"));
                    assertEquals(1,n1.getProperty("count"));
                    List<Relationship> rels = (List<Relationship>) row.get("relationships");
                    Relationship rel = rels.iterator().next();
                    assertEquals("ACTED_IN",rel.getType().name());
                    assertEquals(1,rel.getProperty("count"));
                });
    }
}
