package apoc.schema;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.ConstraintType;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static apoc.util.TestUtil.ignoreException;
import static apoc.util.TestUtil.registerProcedure;
import static apoc.util.TestUtil.singleResultFirstColumn;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.internal.schema.SchemaUserDescription.TOKEN_LABEL;
import static org.neo4j.internal.schema.SchemaUserDescription.TOKEN_REL_TYPE;

/**
 * @author mh
 * @since 12.05.16
 */
public class SchemasTest {

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(GraphDatabaseSettings.procedure_unrestricted, Collections.singletonList("apoc.*"));

    private static void accept(Result result) {
        Map<String, Object> r = result.next();

        assertEquals(":Foo(bar)", r.get("name"));
        assertEquals("ONLINE", r.get("status"));
        assertEquals("Foo", r.get("label"));
        assertEquals("INDEX", r.get("type"));
        assertEquals("bar", ((List<String>) r.get("properties")).get(0));
        assertEquals("NO FAILURE", r.get("failure"));
        assertEquals(100d, r.get("populationProgress"));
        assertEquals(1d, r.get("valuesSelectivity"));
        Assertions.assertThat( r.get( "userDescription").toString() ).contains("name='index_70bffdab', type='GENERAL BTREE', schema=(:Foo {bar}), indexProvider='native-btree-1.0' )");

        assertTrue(!result.hasNext());
    }

    private static void accept2(Result result) {
        Map<String, Object> r = result.next();

        assertEquals(":Foo(bar)", r.get("name"));
        assertEquals("ONLINE", r.get("status"));
        assertEquals("Foo", r.get("label"));
        assertEquals("INDEX", r.get("type"));
        assertEquals("bar", ((List<String>) r.get("properties")).get(0));
        assertEquals("NO FAILURE", r.get("failure"));
        assertEquals(100d, r.get("populationProgress"));
        assertEquals(1d, r.get("valuesSelectivity"));
        Assertions.assertThat( r.get( "userDescription").toString() ).contains( "name='index_70bffdab', type='GENERAL BTREE', schema=(:Foo {bar}), indexProvider='native-btree-1.0' )" );

        r = result.next();

        assertEquals(":Person(name)", r.get("name"));
        assertEquals("ONLINE", r.get("status"));
        assertEquals("Person", r.get("label"));
        assertEquals("INDEX", r.get("type"));
        assertEquals("name", ((List<String>) r.get("properties")).get(0));
        assertEquals("NO FAILURE", r.get("failure"));
        assertEquals(100d, r.get("populationProgress"));
        assertEquals(1d, r.get("valuesSelectivity"));
        Assertions.assertThat( r.get( "userDescription").toString() ).contains( "name='index_5c0607ad', type='GENERAL BTREE', schema=(:Person {name}), indexProvider='native-btree-1.0' )" );

        assertTrue(!result.hasNext());
    }

    @Before
    public void setUp() throws Exception {
        registerProcedure(db, Schemas.class);
        dropSchema();
    }

    @Test
    public void testCreateIndex() throws Exception {
        testCall(db, "CALL apoc.schema.assert({Foo:['bar']},null)", (r) -> {
            assertEquals("Foo", r.get("label"));
            assertEquals("bar", r.get("key"));
            assertEquals(false, r.get("unique"));
            assertEquals("CREATED", r.get("action"));
        });
        try (Transaction tx = db.beginTx()) {
            List<IndexDefinition> indexes = Iterables.asList(tx.schema().getIndexes(Label.label("Foo")));
            assertEquals(1, indexes.size());
            assertEquals("Foo", Iterables.single(indexes.get(0).getLabels()).name());
            assertEquals(asList("bar"), indexes.get(0).getPropertyKeys());
        }
    }

    @Test
    public void testCreateSchema() throws Exception {
        testCall(db, "CALL apoc.schema.assert(null,{Foo:['bar']})", (r) -> {
            assertEquals("Foo", r.get("label"));
            assertEquals("bar", r.get("key"));
            assertEquals(true, r.get("unique"));
            assertEquals("CREATED", r.get("action"));
        });
        try (Transaction tx = db.beginTx()) {
            List<ConstraintDefinition> constraints = Iterables.asList(tx.schema().getConstraints(Label.label("Foo")));
            assertEquals(1, constraints.size());
            ConstraintDefinition constraint = constraints.get(0);
            assertEquals(ConstraintType.UNIQUENESS, constraint.getConstraintType());
            assertEquals("Foo", constraint.getLabel().name());
            assertEquals("bar", Iterables.single(constraint.getPropertyKeys()));
        }
    }

    @Test
    public void testDropIndexWhenUsingDropExisting() throws Exception {
        db.executeTransactionally("CREATE INDEX ON :Foo(bar)");
        db.executeTransactionally("CREATE FULLTEXT INDEX titlesAndDescriptions FOR (n:Movie|Book) ON EACH [n.title, n.description]");
        testCall(db, "CALL apoc.schema.assert(null,null)", (r) -> {
            assertEquals("Foo", r.get("label"));
            assertEquals("bar", r.get("key"));
            assertEquals(false, r.get("unique"));
            assertEquals("DROPPED", r.get("action"));
        });
        try (Transaction tx = db.beginTx()) {
            List<IndexDefinition> indexes = Iterables.asList(tx.schema().getIndexes());
            // the multi-token idx remains
            assertEquals(1, indexes.size());
        }
    }

    @Test
    public void testDropIndexAndCreateIndexWhenUsingDropExisting() throws Exception {
        db.executeTransactionally("CREATE INDEX ON :Foo(bar)");
        testResult(db, "CALL apoc.schema.assert({Bar:['foo']},null)", (result) -> {
            Map<String, Object> r = result.next();
            assertEquals("Foo", r.get("label"));
            assertEquals("bar", r.get("key"));
            assertEquals(false, r.get("unique"));
            assertEquals("DROPPED", r.get("action"));

            r = result.next();
            assertEquals("Bar", r.get("label"));
            assertEquals("foo", r.get("key"));
            assertEquals(false, r.get("unique"));
            assertEquals("CREATED", r.get("action"));
        });
        try (Transaction tx = db.beginTx()) {
            List<IndexDefinition> indexes = Iterables.asList(tx.schema().getIndexes());
            assertEquals(1, indexes.size());
        }
    }

    @Test
    public void testRetainIndexWhenNotUsingDropExisting() throws Exception {
        db.executeTransactionally("CREATE INDEX ON :Foo(bar)");
        testResult(db, "CALL apoc.schema.assert({Bar:['foo', 'bar']}, null, false)", (result) -> {
            Map<String, Object> r = result.next();
            assertEquals("Foo", r.get("label"));
            assertEquals("bar", r.get("key"));
            assertEquals(false, r.get("unique"));
            assertEquals("KEPT", r.get("action"));

            r = result.next();
            assertEquals("Bar", r.get("label"));
            assertEquals("foo", r.get("key"));
            assertEquals(false, r.get("unique"));
            assertEquals("CREATED", r.get("action"));

            r = result.next();
            assertEquals("Bar", r.get("label"));
            assertEquals("bar", r.get("key"));
            assertEquals(false, r.get("unique"));
            assertEquals("CREATED", r.get("action"));
        });
        try (Transaction tx = db.beginTx()) {
            List<IndexDefinition> indexes = Iterables.asList(tx.schema().getIndexes());
            assertEquals(3, indexes.size());
        }
    }

    @Test
    public void testDropSchemaWhenUsingDropExisting() throws Exception {
        db.executeTransactionally("CREATE CONSTRAINT FOR (f:Foo) REQUIRE f.bar IS UNIQUE");
        testCall(db, "CALL apoc.schema.assert(null,null)", (r) -> {
            assertEquals("Foo", r.get("label"));
            assertEquals("bar", r.get("key"));
            assertEquals(true, r.get("unique"));
            assertEquals("DROPPED", r.get("action"));
        });
        try (Transaction tx = db.beginTx()) {
            List<ConstraintDefinition> constraints = Iterables.asList(tx.schema().getConstraints());
            assertEquals(0, constraints.size());
        }
    }

    @Test
    public void testDropSchemaAndCreateSchemaWhenUsingDropExisting() throws Exception {
        db.executeTransactionally("CREATE CONSTRAINT FOR (f:Foo) REQUIRE f.bar IS UNIQUE");
        testResult(db, "CALL apoc.schema.assert(null, {Bar:['foo']})", (result) -> {
            Map<String, Object> r = result.next();
            assertEquals("Foo", r.get("label"));
            assertEquals("bar", r.get("key"));
            assertEquals(true, r.get("unique"));
            assertEquals("DROPPED", r.get("action"));

            r = result.next();
            assertEquals("Bar", r.get("label"));
            assertEquals("foo", r.get("key"));
            assertEquals(true, r.get("unique"));
            assertEquals("CREATED", r.get("action"));
        });
        try (Transaction tx = db.beginTx()) {
            List<ConstraintDefinition> constraints = Iterables.asList(tx.schema().getConstraints());
            assertEquals(1, constraints.size());
        }
    }

    @Test
    public void testRetainSchemaWhenNotUsingDropExisting() throws Exception {
        db.executeTransactionally("CREATE CONSTRAINT FOR (f:Foo) REQUIRE f.bar IS UNIQUE");
        testResult(db, "CALL apoc.schema.assert(null, {Bar:['foo', 'bar']}, false)", (result) -> {
            Map<String, Object> r = result.next();
            assertEquals("Foo", r.get("label"));
            assertEquals("bar", r.get("key"));
            assertEquals(true, r.get("unique"));
            assertEquals("KEPT", r.get("action"));

            r = result.next();
            assertEquals("Bar", r.get("label"));
            assertEquals("foo", r.get("key"));
            assertEquals(true, r.get("unique"));
            assertEquals("CREATED", r.get("action"));

            r = result.next();
            assertEquals("Bar", r.get("label"));
            assertEquals("bar", r.get("key"));
            assertEquals(true, r.get("unique"));
            assertEquals("CREATED", r.get("action"));
        });
        try (Transaction tx = db.beginTx()) {
            List<ConstraintDefinition> constraints = Iterables.asList(tx.schema().getConstraints());
            assertEquals(3, constraints.size());
        }
    }

    @Test
    public void testKeepIndex() throws Exception {
        keepIndexCommon(false);
    }

    @Test
    public void testKeepIndexWithDropExisting() throws Exception {
        keepIndexCommon(true);
    }

    private void keepIndexCommon(boolean dropExisting) {
        db.executeTransactionally("CREATE INDEX ON :Foo(bar)");
        testResult(db, "CALL apoc.schema.assert({Foo:['bar', 'foo']}, null, $drop)",
                Map.of("drop", dropExisting),
                (result) -> {
                    Map<String, Object> r = result.next();
                    assertEquals("Foo", r.get("label"));
                    assertEquals("bar", r.get("key"));
                    assertEquals(false, r.get("unique"));
                    assertEquals("KEPT", r.get("action"));

                    r = result.next();
                    assertEquals("Foo", r.get("label"));
                    assertEquals("foo", r.get("key"));
                    assertEquals(false, r.get("unique"));
                    assertEquals("CREATED", r.get("action"));
                });

        try (Transaction tx = db.beginTx()) {
            List<IndexDefinition> indexes = Iterables.asList(tx.schema().getIndexes());
            assertEquals(2, indexes.size());
        }
    }

    @Test
    public void testKeepSchema() throws Exception {
        db.executeTransactionally("CREATE CONSTRAINT FOR (f:Foo) REQUIRE f.bar IS UNIQUE");
        testResult(db, "CALL apoc.schema.assert(null,{Foo:['bar', 'foo']})", (result) -> {
            Map<String, Object> r = result.next();
            assertEquals("Foo", r.get("label"));
            assertEquals("bar", r.get("key"));
            assertEquals(expectedKeys("bar"), r.get("keys"));
            assertEquals(true, r.get("unique"));
            assertEquals("KEPT", r.get("action"));

            r = result.next();
            assertEquals("Foo", r.get("label"));
            assertEquals("foo", r.get("key"));
            assertEquals(true, r.get("unique"));
            assertEquals("CREATED", r.get("action"));
        });
        try (Transaction tx = db.beginTx()) {
            List<ConstraintDefinition> constraints = Iterables.asList(tx.schema().getConstraints());
            assertEquals(2, constraints.size());
        }
    }

    @Test
    public void testIndexes() {
        db.executeTransactionally("CREATE INDEX ON :Foo(bar)");
        awaitIndexesOnline();
        testResult(db, "CALL apoc.schema.nodes()", (result) -> {
            // Get the index info
            Map<String, Object> r = result.next();

            assertEquals(":Foo(bar)", r.get("name"));
            assertEquals("ONLINE", r.get("status"));
            assertEquals("Foo", r.get("label"));
            assertEquals("INDEX", r.get("type"));
            assertEquals("bar", ((List<String>) r.get("properties")).get(0));
            assertEquals("NO FAILURE", r.get("failure"));
            assertEquals(100d, r.get("populationProgress"));
            assertEquals(1d, r.get("valuesSelectivity"));
            Assertions.assertThat(r.get("userDescription").toString()).contains("name='index_70bffdab', type='GENERAL BTREE', schema=(:Foo {bar}), indexProvider='native-btree-1.0' )");

            assertFalse(result.hasNext());
        });
    }

    @Test
    public void testRelIndex() {
        db.executeTransactionally("CREATE INDEX FOR ()-[r:KNOWS]-() ON (r.id, r.since)");
        awaitIndexesOnline();
        testCall(db, "CALL apoc.schema.relationships()", row -> {
            assertEquals(":KNOWS(id,since)", row.get("name"));
            assertEquals("ONLINE", row.get("status"));
            assertEquals("KNOWS", row.get("relationshipType"));
            assertEquals("INDEX", row.get("type"));
            assertEquals(List.of("id", "since"), row.get("properties"));
        });
    }

    @Test
    public void testIndexExists() {
        db.executeTransactionally("CREATE INDEX ON :Foo(bar)");
        testResult(db, "RETURN apoc.schema.node.indexExists('Foo', ['bar'])", (result) -> {
            Map<String, Object> r = result.next();
            assertEquals(true, r.entrySet().iterator().next().getValue());
        });
    }

    @Test
    public void testIndexNotExists() {
        db.executeTransactionally("CREATE INDEX ON :Foo(bar)");
        testResult(db, "RETURN apoc.schema.node.indexExists('Bar', ['foo'])", (result) -> {
            Map<String, Object> r = result.next();
            assertEquals(false, r.entrySet().iterator().next().getValue());
        });
    }
    
    @Test
    public void testRelationshipExists() {
        db.executeTransactionally("CREATE INDEX rel_index_simple FOR ()-[r:KNOWS]-() ON (r.since)");
        db.executeTransactionally("CREATE INDEX rel_index_composite FOR ()-[r:PURCHASED]-() ON (r.date, r.amount)");
        awaitIndexesOnline();
        
        assertTrue(singleResultFirstColumn(db, "RETURN apoc.schema.relationship.indexExists('KNOWS', ['since'])"));
        assertFalse(singleResultFirstColumn(db, "RETURN apoc.schema.relationship.indexExists('KNOWS', ['dunno'])"));
        // - composite index
        assertTrue(singleResultFirstColumn(db, "RETURN apoc.schema.relationship.indexExists('PURCHASED', ['date', 'amount'])"));
        assertFalse(singleResultFirstColumn(db, "RETURN apoc.schema.relationship.indexExists('PURCHASED', ['date', 'another'])"));
    }

    @Test
    public void testUniquenessConstraintOnNode() {
        db.executeTransactionally("CREATE CONSTRAINT FOR (bar:Bar) REQUIRE bar.foo IS UNIQUE");
        awaitIndexesOnline();

        testResult(db, "CALL apoc.schema.nodes()", (result) -> {
            Map<String, Object> r = result.next();

            assertEquals(":Bar(foo)", r.get("name"));
            assertEquals("Bar", r.get("label"));
            assertEquals("UNIQUENESS", r.get("type"));
            assertEquals("foo", ((List<String>) r.get("properties")).get(0));

            assertFalse(result.hasNext());
        });
    }

    @Test
    public void testIndexAndUniquenessConstraintOnNode() {
        db.executeTransactionally("CREATE INDEX ON :Foo(foo)");
        db.executeTransactionally("CREATE CONSTRAINT FOR (bar:Bar) REQUIRE bar.bar IS UNIQUE");
        awaitIndexesOnline();

        testResult(db, "CALL apoc.schema.nodes()", (result) -> {
            Map<String, Object> r = result.next();

            assertEquals("Bar", r.get("label"));
            assertEquals("UNIQUENESS", r.get("type"));
            assertEquals("bar", ((List<String>) r.get("properties")).get(0));

            r = result.next();
            assertEquals("Foo", r.get("label"));
            assertEquals("INDEX", r.get("type"));
            assertEquals("foo", ((List<String>) r.get("properties")).get(0));
            assertEquals("ONLINE", r.get("status"));

            assertFalse(result.hasNext());
        });
    }

    @Test
    public void testDropCompoundIndexWhenUsingDropExisting() throws Exception {
        db.executeTransactionally("CREATE INDEX ON :Foo(bar,baa)");
        awaitIndexesOnline();
        testResult(db, "CALL apoc.schema.assert(null,null,true)", (result) -> {
            Map<String, Object> r = result.next();
            assertEquals("Foo", r.get("label"));
            assertEquals(expectedKeys("bar", "baa"), r.get("keys"));
            assertEquals(false, r.get("unique"));
            assertEquals("DROPPED", r.get("action"));
        });
        try (Transaction tx = db.beginTx()) {
            List<IndexDefinition> indexes = Iterables.asList(tx.schema().getIndexes());
            assertEquals(0, indexes.size());
        }
    }

    @Test
    public void testDropCompoundIndexAndRecreateWithDropExisting() throws Exception {
        db.executeTransactionally("CREATE INDEX ON :Foo(bar,baa)");
        awaitIndexesOnline();
        testCall(db, "CALL apoc.schema.assert({Foo:[['bar','baa']]},null,true)", (r) -> {
            assertEquals("Foo", r.get("label"));
            assertEquals(expectedKeys("bar", "baa"), r.get("keys"));
            assertEquals(false, r.get("unique"));
            assertEquals("KEPT", r.get("action"));
        });
        try (Transaction tx = db.beginTx()) {
            List<IndexDefinition> indexes = Iterables.asList(tx.schema().getIndexes());
            assertEquals(1, indexes.size());
        }
    }

    @Test
    public void testDoesntDropCompoundIndexWhenSupplyingSameCompoundIndex() throws Exception {
        db.executeTransactionally("CREATE INDEX ON :Foo(bar,baa)");
        awaitIndexesOnline();
        testResult(db, "CALL apoc.schema.assert({Foo:[['bar','baa']]},null,false)", (result) -> {
            Map<String, Object> r = result.next();
            assertEquals("Foo", r.get("label"));
            assertEquals(expectedKeys("bar", "baa"), r.get("keys"));
            assertEquals(false, r.get("unique"));
            assertEquals("KEPT", r.get("action"));
        });
        try (Transaction tx = db.beginTx()) {
            List<IndexDefinition> indexes = Iterables.asList(tx.schema().getIndexes());
            assertEquals(1, indexes.size());
        }
    }

    @Test
    public void testCompoundIndexDoesntAllowCypherInjection() throws Exception {
        awaitIndexesOnline();
        testResult(db, "CALL apoc.schema.assert({Foo:[['bar`) MATCH (n) DETACH DELETE n; //','baa']]},null,false)", (result) -> {
            Map<String, Object> r = result.next();
            assertEquals("Foo", r.get("label"));
            assertEquals(expectedKeys("bar`) MATCH (n) DETACH DELETE n; //", "baa"), r.get("keys"));
            assertEquals(false, r.get("unique"));
        });
        try (Transaction tx = db.beginTx()) {
            List<IndexDefinition> indexes = Iterables.asList(tx.schema().getIndexes());
            assertEquals(1, indexes.size());
        }
    }

    /*
        This is only for 3.2+
    */
    @Test
    public void testKeepCompoundIndex() throws Exception {
        testKeepCompoundCommon(false);
    }
    
    @Test
    public void testKeepCompoundIndexWithDropExisting() throws Exception {
        testKeepCompoundCommon(true);
    }

    private void testKeepCompoundCommon(boolean dropExisting) {
        db.executeTransactionally("CREATE INDEX ON :Foo(bar,baa)");
        awaitIndexesOnline();
        testResult(db, "CALL apoc.schema.assert({Foo:[['bar','baa'], ['foo','faa']]},null,$drop)", 
                Map.of("drop", dropExisting), 
                (result) -> {
            Map<String, Object> r = result.next();
            assertEquals("Foo", r.get("label"));
            assertEquals(expectedKeys("bar", "baa"), r.get("keys"));
            assertEquals(false, r.get("unique"));
            assertEquals("KEPT", r.get("action"));

            r = result.next();
            assertEquals("Foo", r.get("label"));
            assertEquals(expectedKeys("foo", "faa"), r.get("keys"));
            assertEquals(false, r.get("unique"));
            assertEquals("CREATED", r.get("action"));
        });
        
        try (Transaction tx = db.beginTx()) {
            List<IndexDefinition> indexes = Iterables.asList(tx.schema().getIndexes());
            assertEquals(2, indexes.size());
        }
    }

    @Test
    public void testDropIndexAndCreateCompoundIndexWhenUsingDropExisting() throws Exception {
        db.executeTransactionally("CREATE INDEX ON :Foo(bar)");
        awaitIndexesOnline();
        testResult(db, "CALL apoc.schema.assert({Bar:[['foo','bar']]},null)", (result) -> {
            Map<String, Object> r = result.next();
            assertEquals("Foo", r.get("label"));
            assertEquals("bar", r.get("key"));
            assertEquals(false, r.get("unique"));
            assertEquals("DROPPED", r.get("action"));

            r = result.next();
            assertEquals("Bar", r.get("label"));
            assertEquals(expectedKeys("foo", "bar"), r.get("keys"));
            assertEquals(false, r.get("unique"));
            assertEquals("CREATED", r.get("action"));
        });
        try (Transaction tx = db.beginTx()) {
            List<IndexDefinition> indexes = Iterables.asList(tx.schema().getIndexes());
            assertEquals(1, indexes.size());
        }
    }

    @Test
    public void testAssertWithFullTextIndexes() {
        db.executeTransactionally("CALL db.index.fulltext.createNodeIndex('fullIdxNode', ['Moon', 'Blah'], ['weightProp', 'anotherProp'])");
        db.executeTransactionally("CALL db.index.fulltext.createRelationshipIndex('fullIdxRel', ['TYPE_1', 'TYPE_2'], ['alpha', 'beta'])");
        // fulltext with single label, should return label field as string
        db.executeTransactionally("CALL db.index.fulltext.createNodeIndex('fullIdxNodeSingle', ['Asd'], ['uno', 'due'])");
        awaitIndexesOnline();
        testResult(db, "CALL apoc.schema.assert({Bar:[['foo','bar']]}, {One:['two']}) " +
                "YIELD label, key, keys, unique, action RETURN * ORDER BY label", (result) -> {
            Map<String, Object> r = result.next();
            assertEquals("Asd", r.get("label"));
            assertEquals(expectedKeys("uno", "due"), r.get("keys"));
            assertEquals(false, r.get("unique"));
            assertEquals("DROPPED", r.get("action"));

            r = result.next();
            assertEquals("Bar", r.get("label"));
            assertEquals(expectedKeys("foo", "bar"), r.get("keys"));
            assertEquals(false, r.get("unique"));
            assertEquals("CREATED", r.get("action"));

            r = result.next();
            assertEquals("One", r.get("label"));
            assertEquals("two", r.get("key"));
            assertEquals(true, r.get("unique"));
            assertEquals("CREATED", r.get("action"));
            assertFalse(result.hasNext());
        });
        try (Transaction tx = db.beginTx()) {
            List<IndexDefinition> indexes = Iterables.asList(tx.schema().getIndexes());
            assertEquals(4, indexes.size());
            List<ConstraintDefinition> constraints = Iterables.asList(tx.schema().getConstraints());
            assertEquals(1, constraints.size());
        }

    }

    @Test
    public void testDropCompoundIndexAndCreateCompoundIndexWhenUsingDropExisting() throws Exception {
        db.executeTransactionally("CREATE INDEX ON :Foo(bar,baa)");
        awaitIndexesOnline();
        testCall(db, "CALL apoc.schema.assert({Foo:[['bar','baa']]},null)", (r) -> {
            assertEquals("Foo", r.get("label"));
            assertEquals(expectedKeys("bar","baa"), r.get("keys"));
            assertEquals(false, r.get("unique"));
            assertEquals("KEPT", r.get("action"));
        });
        try (Transaction tx = db.beginTx()) {
            List<IndexDefinition> indexes = Iterables.asList(tx.schema().getIndexes());
            assertEquals(1, indexes.size());
        }
    }

    private List<String> expectedKeys(String... keys){
        return asList(keys);
    }


    @Test
    public void testIndexesOneLabel() {
        db.executeTransactionally("CREATE INDEX ON :Foo(bar)");
        db.executeTransactionally("CREATE INDEX ON :Bar(foo)");
        db.executeTransactionally("CREATE INDEX ON :Person(name)");
        db.executeTransactionally("CREATE INDEX ON :Movie(title)");
        awaitIndexesOnline();
        testResult(db, "CALL apoc.schema.nodes({labels:['Foo']})", // Get the index info
                SchemasTest::accept);
    }

    private void awaitIndexesOnline() {
        try (Transaction tx = db.beginTx()) {
            tx.schema().awaitIndexesOnline(5, TimeUnit.SECONDS);
            tx.commit();
        }
    }

    @Test
    public void testIndexesMoreLabels() {
        db.executeTransactionally("CREATE INDEX ON :Foo(bar)");
        db.executeTransactionally("CREATE INDEX ON :Bar(foo)");
        db.executeTransactionally("CREATE INDEX ON :Person(name)");
        db.executeTransactionally("CREATE INDEX ON :Movie(title)");
        awaitIndexesOnline();
        testResult(db, "CALL apoc.schema.nodes({labels:['Foo', 'Person']})", // Get the index info
                SchemasTest::accept2);
    }

    @Test
    public void testSchemaRelationshipsExclude() {
        ignoreException(() -> {
            db.executeTransactionally("CREATE CONSTRAINT FOR ()-[like:LIKED]-() REQUIRE exists(like.day)");
            testResult(db, "CALL apoc.schema.relationships({excludeRelationships:['LIKED']})", (result) -> assertFalse(result.hasNext()));
        }, QueryExecutionException.class);
    }

    @Test
    public void testSchemaNodesExclude() {
        ignoreException(() -> {
            db.executeTransactionally("CREATE CONSTRAINT FOR (book:Book) REQUIRE book.isbn IS UNIQUE");
            testResult(db, "CALL apoc.schema.nodes({excludeLabels:['Book']})", (result) -> assertFalse(result.hasNext()));

        }, QueryExecutionException.class);
    }

    @Test(expected = QueryExecutionException.class)
    public void testIndexesLabelsAndExcludeLabelsValuatedShouldFail() {
        db.executeTransactionally("CREATE INDEX ON :Foo(bar)");
        db.executeTransactionally("CREATE INDEX ON :Bar(foo)");
        db.executeTransactionally("CREATE INDEX ON :Person(name)");
        db.executeTransactionally("CREATE INDEX ON :Movie(title)");
        awaitIndexesOnline();
        try (Transaction tx = db.beginTx()) {
            tx.schema().awaitIndexesOnline(5, TimeUnit.SECONDS);
            tx.commit();
            testResult(db, "CALL apoc.schema.nodes({labels:['Foo', 'Person', 'Bar'], excludeLabels:['Bar']})", (result) -> {});
        } catch (IllegalArgumentException e) {
            Throwable except = ExceptionUtils.getRootCause(e);
            assertTrue(except instanceof IllegalArgumentException);
            assertEquals("Parameters labels and excludelabels are both valuated. Please check parameters and valuate only one.", except.getMessage());
            throw e;
        }

    }

    @Test(expected = QueryExecutionException.class)
    public void testConstraintsRelationshipsAndExcludeRelationshipsValuatedShouldFail() {
        db.executeTransactionally("CREATE CONSTRAINT FOR ()-[like:LIKED]-() REQUIRE exists(like.day)");
        db.executeTransactionally("CREATE CONSTRAINT FOR ()-[knows:SINCE]-() REQUIRE exists(since.year)");
        awaitIndexesOnline();
        try (Transaction tx = db.beginTx()) {
            tx.schema().awaitIndexesOnline(5, TimeUnit.SECONDS);
            tx.commit();
            testResult(db, "CALL apoc.schema.relationships({relationships:['LIKED'], excludeRelationships:['SINCE']})", (result) -> {});
        } catch (IllegalArgumentException e) {
            Throwable except = ExceptionUtils.getRootCause(e);
            assertTrue(except instanceof IllegalArgumentException);
            assertEquals("Parameters relationships and excluderelationships are both valuated. Please check parameters and valuate only one.", except.getMessage());
            throw e;
        }
    }

    @Test
    public void testLookupIndexes() {
        db.executeTransactionally("CREATE LOOKUP INDEX node_label_lookup_index FOR (n) ON EACH labels(n)");
        db.executeTransactionally("CREATE LOOKUP INDEX rel_type_lookup_index FOR ()-[r]-() ON EACH type(r)");
        awaitIndexesOnline();

        testCall(db, "CALL apoc.schema.nodes()", (row) -> {
            assertEquals(":" + TOKEN_LABEL + "()", row.get("name"));
            assertEquals("ONLINE", row.get("status"));
            assertEquals(TOKEN_LABEL, row.get("label"));
            assertEquals("INDEX", row.get("type"));
            assertTrue(((List)row.get("properties")).isEmpty());
            assertEquals("NO FAILURE", row.get("failure"));
            assertEquals(100d, row.get("populationProgress"));
            assertEquals(1d, row.get("valuesSelectivity"));
            assertTrue(row.get("userDescription").toString().contains("name='node_label_lookup_index', type='TOKEN LOOKUP', schema=(:<any-labels>), indexProvider='token-lookup-1.0' )"));
        });
        testCall(db, "CALL apoc.schema.relationships()", (row) -> {
            assertEquals(":" + TOKEN_REL_TYPE + "()", row.get("name"));
            assertEquals("ONLINE", row.get("status"));
            assertEquals("INDEX", row.get("type"));
            assertEquals(TOKEN_REL_TYPE, row.get("relationshipType"));
            assertTrue(((List)row.get("properties")).isEmpty());
        });
    }

    private void dropSchema()
    {
        try(Transaction tx = db.beginTx()) {
            Schema schema = tx.schema();
            schema.getConstraints().forEach(ConstraintDefinition::drop);
            schema.getIndexes().forEach(IndexDefinition::drop);
            tx.commit();
        }
    }

    @Test
    public void testIndexesWithMultipleLabelsAndRelTypes() {
        db.executeTransactionally("CALL db.index.fulltext.createNodeIndex('fullIdxNode', ['Blah', 'Moon'], ['weightProp', 'anotherProp'])");
        db.executeTransactionally("CALL db.index.fulltext.createRelationshipIndex('fullIdxRel', ['TYPE_1', 'TYPE_2'], ['alpha', 'beta'])");
        awaitIndexesOnline();
        
        testResult(db, "CALL apoc.schema.nodes()", (result) -> {
            Map<String, Object> r = result.next();
            assertEquals(":[Blah, Moon],(weightProp,anotherProp)", r.get("name"));
            assertEquals("ONLINE", r.get("status"));
            assertEquals(List.of("Blah", "Moon"), r.get("label"));
            assertEquals("INDEX", r.get("type"));
            assertEquals(List.of("weightProp", "anotherProp"), r.get("properties"));
            assertEquals("NO FAILURE", r.get("failure"));
            assertEquals(100d, r.get("populationProgress"));
            assertEquals(1d, r.get("valuesSelectivity"));
            final long indexId = db.executeTransactionally("CALL db.indexes() YIELD id, name WHERE name = $indexName RETURN id",
                    Map.of("indexName", "fullIdxNode"), res -> res.<Long>columnAs("id").next());
            String expectedIndexDescription = String.format("Index( id=%s, name='fullIdxNode', type='GENERAL FULLTEXT', " +
                    "schema=(:Blah:Moon {weightProp, anotherProp}), indexProvider='fulltext-1.0' )", indexId);
            assertEquals(expectedIndexDescription, r.get("userDescription"));
            assertFalse(result.hasNext());
        });
        
        testResult(db, "CALL apoc.schema.relationships()", (result) -> {
            Map<String, Object> r = result.next();
            assertEquals(":[TYPE_1, TYPE_2],(alpha,beta)", r.get("name"));
            assertEquals("ONLINE", r.get("status"));
            assertEquals(List.of("TYPE_1", "TYPE_2"), r.get("relationshipType"));
            assertEquals(List.of("alpha", "beta"), r.get("properties"));
            assertEquals("INDEX", r.get("type"));
            assertFalse(result.hasNext());
        });
    }
}
