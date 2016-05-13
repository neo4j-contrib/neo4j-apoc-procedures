package apoc.schema;

import apoc.meta.Meta;
import apoc.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.List;

import static apoc.util.TestUtil.testCall;
import static java.util.Arrays.asList;
import static org.junit.Assert.*;

/**
 * @author mh
 * @since 12.05.16
 */
public class SchemasTest {
    private GraphDatabaseService db;

    @Before
    public void setUp() throws Exception {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        TestUtil.registerProcedure(db,Schemas.class); }

    @After
    public void tearDown() {
        db.shutdown();
    }

    @Test
    @Ignore("Hangs indefinitely")
    public void testCreateIndex() throws Exception {
        testCall(db,"CALL apoc.schema.assert([{Foo:['bar']}],null)", (r) -> {
            assertEquals("Foo",r.get("label"));
            assertEquals(asList("bar"),r.get("keys"));
            assertEquals(false,r.get("unique"));
            assertEquals("CREATED",r.get("action"));
        });
        try (Transaction tx = db.beginTx()) {
            List<IndexDefinition> indexes = Iterables.asList(db.schema().getIndexes(Label.label("Foo")));
            assertEquals(1, indexes.size());
            assertEquals("Foo", indexes.get(0).getLabel().name());
            assertEquals(asList("bar"), indexes.get(0).getPropertyKeys());
        }
    }
}
