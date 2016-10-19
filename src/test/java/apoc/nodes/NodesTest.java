package apoc.nodes;

import apoc.coll.Coll;
import apoc.util.TestUtil;
import org.junit.*;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.Assert.*;

/**
 * @author mh
 * @since 18.08.16
 */
public class NodesTest {

    private GraphDatabaseService db;
    @Before
    public void setUp() throws Exception {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        TestUtil.registerProcedure(db,Nodes.class);
    }
    @After
    public void tearDown() {
        db.shutdown();
    }

    @Test
    public void link() throws Exception {
        db.execute("UNWIND range(1,10) as id CREATE (n:Foo {id:id}) WITH collect(n) as nodes call apoc.nodes.link(nodes,'BAR') RETURN size(nodes) as len").close();

        ResourceIterator<Long> it = db.execute("MATCH (n:Foo {id:1})-[r:BAR*9]->() RETURN size(r) as len").columnAs("len");
        assertEquals(9L,(long)it.next());
        it.close();
    }
    @Test
    public void delete() throws Exception {
        db.execute("UNWIND range(1,100) as id CREATE (n:Foo {id:id})-[:X]->(n)").close();
        ResourceIterator<Long> it =  db.execute("MATCH (n:Foo) WITH collect(n) as nodes call apoc.nodes.delete(nodes,1) YIELD value as count RETURN count").columnAs("count");
        long count = it.next();
        it.close();

        assertEquals(100L,count);

        it = db.execute("MATCH (n:Foo) RETURN count(*) as c").columnAs("c");
        assertEquals(0L,(long)it.next());
        it.close();
    }
}
