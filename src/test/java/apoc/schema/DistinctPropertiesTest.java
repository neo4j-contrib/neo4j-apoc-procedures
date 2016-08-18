package apoc.schema;

import apoc.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.concurrent.TimeUnit;

import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static org.junit.Assert.assertEquals;
import static java.util.Arrays.asList;

public class DistinctPropertiesTest {
    private GraphDatabaseService db;

    @Before
    public void setUp() throws Exception {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        TestUtil.registerProcedure(db,Properties.class);
    }

    @After
    public void tearDown() {
        db.shutdown();
    }

    @Test
    public void testDistinctProperties() throws Exception {
        db.execute("CREATE INDEX ON :Foo(bar)").close();
        db.execute("CREATE (f:Foo {bar:'one'}), (f2a:Foo {bar:'two'}), (f2b:Foo {bar:'two'})").close();
        String label = "Foo";
        String key = "bar";
        try (Transaction tx = db.beginTx()) {
            db.schema().awaitIndexesOnline(2, TimeUnit.SECONDS);
            tx.success();
        }

        testCall(db,"CALL apoc.schema.properties.distinct({label}, {key})",
                map("label",label,"key",key),
                (row) -> assertEquals(asList("one","two"), row.get("value"))
        );
    }

}
