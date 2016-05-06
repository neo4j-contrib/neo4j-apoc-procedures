package apoc.get;

import apoc.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.*;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.Collection;

import static apoc.util.MapUtil.map;
import static org.junit.Assert.*;

/**
 * @author mh
 * @since 16.04.16
 */
public class GetTest {

    private GraphDatabaseService db;

    @Before
    public void setUp() throws Exception {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        TestUtil.registerProcedure(db,Get.class);
    }
    @After
    public void tearDown() {
        db.shutdown();
    }


    @Test
    public void testNodes() throws Exception {
        Collection<Object> ids = Iterators.asSet(db.execute("UNWIND range(0,2) as id CREATE (n:Node {id:id}) return id(n) as id").columnAs("id"));
        TestUtil.testResult(db, "CALL apoc.get.nodes({ids})", map("ids",ids), r -> {
            assertEquals(true, ids.contains(((Node) r.next().get("node")).getId()));
            assertEquals(true, ids.contains(((Node) r.next().get("node")).getId()));
            assertEquals(true, ids.contains(((Node) r.next().get("node")).getId()));
        });
    }

    @Test
    public void testRels() throws Exception {
        Collection<Object> ids = Iterators.asSet(db.execute("CREATE (n) WITH n UNWIND range(0,2) as id CREATE (n)-[r:KNOWS]->(n) return id(r) as id").columnAs("id"));
        TestUtil.testResult(db, "CALL apoc.get.rels({ids})", map("ids",ids), r -> {
            assertEquals(true, ids.contains(((Relationship) r.next().get("rel")).getId()));
            assertEquals(true, ids.contains(((Relationship) r.next().get("rel")).getId()));
            assertEquals(true, ids.contains(((Relationship) r.next().get("rel")).getId()));
        });
    }
}
