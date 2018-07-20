package apoc.label;

import apoc.util.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.TestGraphDatabaseFactory;

import static apoc.util.TestUtil.testCall;
import static org.junit.Assert.assertEquals;

public class LabelTest {

    private static GraphDatabaseService db;

    @BeforeClass
    public static void setUp() throws Exception {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        TestUtil.registerProcedure(db, Label.class);
    }

    @AfterClass
    public static void tearDown() {
        db.shutdown();
    }

    @Test
    public void testVerifyNodeLabelExistance() throws Exception {

        db.execute("create (a:Person{name:'Foo'})");

        testCall(db, "MATCH (a) RETURN apoc.label.exists(a, 'Person') as value",
                (row) -> {
                    assertEquals(true, row.get("value"));
                });
        testCall(db, "MATCH (a) RETURN apoc.label.exists(a, 'Dog') as value",
                (row) -> {
                    assertEquals(false, row.get("value"));
                });
    }

    @Test
    public void testVerifyRelTypeExistance() throws Exception {


        db.execute("create (a:Person{name:'Foo'}), (b:Person{name:'Bar'}), (a)-[:LOVE{since:2010}]->(b)");

        testCall(db, "MATCH ()-[a]->() RETURN apoc.label.exists(a, 'LOVE') as value",
                (row) -> {
                    assertEquals(true, row.get("value"));
                });
        testCall(db, "MATCH ()-[a]->() RETURN apoc.label.exists(a, 'LIVES_IN') as value",
                (row) -> {
                    assertEquals(false, row.get("value"));
                });

    }
}