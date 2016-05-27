package apoc.schema;

import apoc.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.ConstraintType;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.List;

import static apoc.util.TestUtil.testCall;
import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
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
//        TestUtil.registerProcedure(db,AssertSchemaProcedure.class);
    }

    @After
    public void tearDown() {
        db.shutdown();
    }

    @Test
    public void testCreateIndex() throws Exception {
        testCall(db,"CALL apoc.schema.assert({Foo:['bar']},null)", (r) -> {
            assertEquals("Foo",r.get("label"));
            assertEquals("bar",r.get("key"));
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

    @Test
    public void testCreateSchema() throws Exception {
        testCall(db,"CALL apoc.schema.assert(null,{Foo:['bar']})", (r) -> {
            assertEquals("Foo",r.get("label"));
            assertEquals("bar",r.get("key"));
            assertEquals(true,r.get("unique"));
            assertEquals("CREATED",r.get("action"));
        });
        try (Transaction tx = db.beginTx()) {
            List<ConstraintDefinition> constraints = Iterables.asList(db.schema().getConstraints(Label.label("Foo")));
            assertEquals(1, constraints.size());
            ConstraintDefinition constraint = constraints.get(0);
            assertEquals(ConstraintType.UNIQUENESS, constraint.getConstraintType());
            assertEquals("Foo", constraint.getLabel().name());
            assertEquals(singleton("bar"), constraint.getPropertyKeys());
        }
    }

    @Test
    public void testDropIndex() throws Exception {
        db.execute("CREATE INDEX ON :Foo(bar)").close();
        testCall(db,"CALL apoc.schema.assert(null,null)", (r) -> {
            assertEquals("Foo",r.get("label"));
            assertEquals("bar",r.get("key"));
            assertEquals(false,r.get("unique"));
            assertEquals("DROPPED",r.get("action"));
        });
        try (Transaction tx = db.beginTx()) {
            List<IndexDefinition> indexes = Iterables.asList(db.schema().getIndexes());
            assertEquals(0, indexes.size());
        }
    }

    @Test
    public void testDropSchema() throws Exception {
        db.execute("CREATE CONSTRAINT ON (f:Foo) assert f.bar is unique").close();
        testCall(db,"CALL apoc.schema.assert(null,null)", (r) -> {
            assertEquals("Foo",r.get("label"));
            assertEquals("bar",r.get("key"));
            assertEquals(true,r.get("unique"));
            assertEquals("DROPPED",r.get("action"));
        });
        try (Transaction tx = db.beginTx()) {
            List<ConstraintDefinition> constraints = Iterables.asList(db.schema().getConstraints());
            assertEquals(0, constraints.size());
        }
    }
    @Test
    public void testKeepIndex() throws Exception {
        db.execute("CREATE INDEX ON :Foo(bar)").close();
        testCall(db,"CALL apoc.schema.assert({Foo:['bar']},null)", (r) -> {
            assertEquals("Foo",r.get("label"));
            assertEquals("bar",r.get("key"));
            assertEquals(false,r.get("unique"));
            assertEquals("KEPT",r.get("action"));
        });
        try (Transaction tx = db.beginTx()) {
            List<IndexDefinition> indexes = Iterables.asList(db.schema().getIndexes());
            assertEquals(1, indexes.size());
        }
    }

    @Test
    public void testKeepSchema() throws Exception {
        db.execute("CREATE CONSTRAINT ON (f:Foo) assert f.bar is unique").close();
        testCall(db,"CALL apoc.schema.assert(null,{Foo:['bar']})", (r) -> {
            assertEquals("Foo",r.get("label"));
            assertEquals("bar",r.get("key"));
            assertEquals(true,r.get("unique"));
            assertEquals("KEPT",r.get("action"));
        });
        try (Transaction tx = db.beginTx()) {
            List<ConstraintDefinition> constraints = Iterables.asList(db.schema().getConstraints());
            assertEquals(1, constraints.size());
        }
    }
}
