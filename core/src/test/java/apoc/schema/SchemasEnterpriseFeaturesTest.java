package apoc.schema;

import apoc.util.Neo4jContainerExtension;
import apoc.util.TestUtil;
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static apoc.util.TestContainerUtil.createEnterpriseDB;
import static apoc.util.TestContainerUtil.testCall;
import static apoc.util.TestContainerUtil.testResult;
import static apoc.util.TestUtil.isRunningInCI;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeNotNull;
import static org.junit.Assume.assumeTrue;

/**
 * @author as
 * @since 12.02.19
 */
public class SchemasEnterpriseFeaturesTest {

    private static Neo4jContainerExtension neo4jContainer;
    private static Session session;

    @BeforeClass
    public static void beforeAll() {
        assumeFalse(isRunningInCI());
        TestUtil.ignoreException(() -> {
            // We build the project, the artifact will be placed into ./build/libs
            neo4jContainer = createEnterpriseDB(true);
            neo4jContainer.start();
        }, Exception.class);
        assumeNotNull(neo4jContainer);
        assumeTrue("Neo4j Instance should be up-and-running", neo4jContainer.isRunning());
        session = neo4jContainer.getSession();
    }

    @AfterClass
    public static void afterAll() {
        if (neo4jContainer != null && neo4jContainer.isRunning()) {
            session.close();
            neo4jContainer.close();
        }
    }

    @After
    public void removeAllConstraints() {
        session.writeTransaction(tx -> {
            tx.run("CALL apoc.schema.assert({},{})");
            tx.commit();
            return null;
        });
    }

    @Test
    public void testKeptNodeKeyAndUniqueConstraintIfExists() {
        String query = "CALL apoc.schema.assert(null,{Foo:[['foo','bar']]}, false)";
        testResult(session, query, (result) -> {
            Map<String, Object> r = result.next();
            assertEquals("Foo", r.get("label"));
            assertEquals(expectedKeys("foo", "bar"), r.get("keys"));
            assertEquals(true, r.get("unique"));
            assertEquals("CREATED", r.get("action"));
            assertFalse(result.hasNext());
        });

        testResult(session, query, (result) -> {
            Map<String, Object> r = result.next();
            assertEquals("Foo", r.get("label"));
            assertEquals(expectedKeys("foo", "bar"), r.get("keys"));
            assertEquals(true, r.get("unique"));
            assertEquals("KEPT", r.get("action"));
            assertFalse(result.hasNext());
        });

        session.readTransaction(tx -> {
            List<Record> result = tx.run("SHOW CONSTRAINTS YIELD createStatement").list();
            assertEquals(1, result.size());
            Map<String, Object> firstResult = result.get(0).asMap();
            assertThat( (String) firstResult.get( "createStatement" ) )
                    .contains( "CREATE CONSTRAINT", "FOR (n:`Foo`) REQUIRE (n.`foo`, n.`bar`) IS NODE KEY" );
            tx.commit();
            return null;
        });
    }

    @Test
    public void testKeptNodeKeyAndUniqueConstraintIfExistsAndDropExistingIsFalse() {
        String query = "CALL apoc.schema.assert(null,{Foo:[['foo','bar']]}, false)";
        testResult(session, query, (result) -> {
            Map<String, Object> r = result.next();
            assertEquals("Foo", r.get("label"));
            assertEquals(expectedKeys("foo", "bar"), r.get("keys"));
            assertEquals(true, r.get("unique"));
            assertEquals("CREATED", r.get("action"));
            assertFalse(result.hasNext());
        });
        testResult(session, "CALL apoc.schema.assert(null,{Foo:[['bar','foo']]}, false)", (result) -> {
            Map<String, Object> r = result.next();
            assertEquals("Foo", r.get("label"));
            assertEquals(expectedKeys("foo", "bar"), r.get("keys"));
            assertEquals(true, r.get("unique"));
            assertEquals("KEPT", r.get("action"));
            r = result.next();
            assertEquals("Foo", r.get("label"));
            assertEquals(expectedKeys("bar", "foo"), r.get("keys"));
            assertEquals(true, r.get("unique"));
            assertEquals("CREATED", r.get("action"));
            assertFalse(result.hasNext());
        });

        session.readTransaction(tx -> {
            List<Record> result = tx.run("SHOW CONSTRAINTS YIELD createStatement").list();
            assertEquals(2, result.size());
            List<String> actualDescriptions = result.stream()
                    .map(record -> (String) record.asMap().get("createStatement"))
                    .collect(Collectors.toList());
            List<String> expectedDescriptions = List.of(
                    "FOR (n:`Foo`) REQUIRE (n.`foo`, n.`bar`) IS NODE KEY",
                    "FOR (n:`Foo`) REQUIRE (n.`bar`, n.`foo`) IS NODE KEY" );
            assertMatchesAll( expectedDescriptions, actualDescriptions );
            tx.commit();
            return null;
        });
    }

    @Test
    public void testCreateUniqueAndIsNodeKeyConstraintInSameLabel() {
        testResult(session, "CALL apoc.schema.assert(null,{Galileo: [['newton', 'tesla'], 'curie']}, false)", (result) -> {
            Map<String, Object> r = result.next();
            assertEquals("Galileo", r.get("label"));
            assertEquals(expectedKeys("newton", "tesla"), r.get("keys"));
            assertEquals(true, r.get("unique"));
            assertEquals("CREATED", r.get("action"));
            r = result.next();
            assertEquals("Galileo", r.get("label"));
            assertEquals(expectedKeys("curie"), r.get("keys"));
            assertEquals(true, r.get("unique"));
            assertEquals("CREATED", r.get("action"));
            assertFalse(result.hasNext());
        });

        session.readTransaction(tx -> {
            List<Record> result = tx.run("SHOW CONSTRAINTS YIELD createStatement").list();
            assertEquals(2, result.size());
            List<String> actualDescriptions = result.stream()
                    .map(record -> (String) record.asMap().get("createStatement"))
                    .collect(Collectors.toList());
            List<String> expectedDescriptions = List.of(
                    "FOR (n:`Galileo`) REQUIRE (n.`newton`, n.`tesla`) IS NODE KEY",
                    "FOR (n:`Galileo`) REQUIRE (n.`curie`) IS UNIQUE");
            assertMatchesAll( expectedDescriptions, actualDescriptions );
            tx.commit();
            return null;
        });
    }

    @Test
    public void testDropNodeKeyConstraintAndCreateNodeKeyConstraintWhenUsingDropExistingOnlyIfNotExist() {
        session.writeTransaction(tx -> {
            tx.run("CREATE CONSTRAINT FOR (f:Foo) REQUIRE (f.bar,f.foo) IS NODE KEY");
            tx.commit();
            return null;
        });
        testResult(session, "CALL apoc.schema.assert(null,{Foo:[['bar','foo']]})", (result) -> {
            Map<String, Object> r = result.next();
            assertEquals("Foo", r.get("label"));
            assertEquals(expectedKeys("bar","foo"), r.get("keys"));
            assertEquals(true, r.get("unique"));
            assertEquals("KEPT", r.get("action"));
        });

        testResult(session, "CALL apoc.schema.assert(null,{Foo:[['baa','baz']]})", (result) -> {
            Map<String, Object> r = result.next();
            assertEquals("Foo", r.get("label"));
            assertEquals(expectedKeys("bar","foo"), r.get("keys"));
            assertEquals(true, r.get("unique"));
            assertEquals("DROPPED", r.get("action"));

            r = result.next();
            assertEquals("Foo", r.get("label"));
            assertEquals(expectedKeys("baa","baz"), r.get("keys"));
            assertEquals(true, r.get("unique"));
            assertEquals("CREATED", r.get("action"));

            assertFalse(result.hasNext());
        });

        session.readTransaction(tx -> {
            List<Record> result = tx.run("SHOW CONSTRAINTS YIELD createStatement").list();
            assertEquals(1, result.size());
            Map<String, Object> firstResult = result.get(0).asMap();
            assertThat( (String) firstResult.get( "createStatement" ) )
                    .contains( "CREATE CONSTRAINT", "FOR (n:`Foo`) REQUIRE (n.`baa`, n.`baz`) IS NODE KEY" );
            tx.commit();
            return null;
        });
    }

    @Test
    public void testDropSchemaWithNodeKeyConstraintWhenUsingDropExisting() {
        session.writeTransaction(tx -> {
            tx.run("CREATE CONSTRAINT FOR (f:Foo) REQUIRE (f.foo, f.bar) IS NODE KEY");
            tx.commit();
            return null;
        });
        testCall(session, "CALL apoc.schema.assert(null,null)", (r) -> {
            assertEquals("Foo", r.get("label"));
            assertEquals(expectedKeys("foo", "bar"), r.get("keys"));
            assertEquals(true, r.get("unique"));
            assertEquals("DROPPED", r.get("action"));
        });

        session.readTransaction(tx -> {
            List<Record> result = tx.run("SHOW CONSTRAINTS").list();
            assertEquals(0, result.size());
            tx.commit();
            return null;
        });
    }

    @Test
    public void testDropConstraintExistsPropertyNode() {
        session.writeTransaction(tx -> {
            tx.run("CREATE CONSTRAINT FOR (m:Movie) REQUIRE (m.title) IS NOT NULL");
            tx.commit();
            return null;
        });
        testCall(session, "CALL apoc.schema.assert({},{})", (r) -> {
            assertEquals("Movie", r.get("label"));
            assertEquals(expectedKeys("title"), r.get("keys"));
            assertTrue("should be unique", (boolean) r.get("unique"));
            assertEquals("DROPPED", r.get("action"));
        });

        session.readTransaction(tx -> {
            List<Record> result = tx.run("SHOW CONSTRAINTS").list();
            assertEquals(0, result.size());
            tx.commit();
            return null;
        });
    }

    @Test
    public void testDropConstraintExistsPropertyRelationship() {
        session.writeTransaction(tx -> {
            tx.run("CREATE CONSTRAINT FOR ()-[acted:Acted]->() REQUIRE (acted.since) IS NOT NULL");
            tx.commit();
            return null;
        });

        testCall(session, "CALL apoc.schema.assert({},{})", (r) -> {
            assertEquals("Acted", r.get("label"));
            assertEquals(expectedKeys("since"), r.get("keys"));
            assertTrue("should be unique", (boolean) r.get("unique"));
            assertEquals("DROPPED", r.get("action"));
        });

        session.readTransaction(tx -> {
            List<Record> result = tx.run("SHOW CONSTRAINTS").list();
            assertEquals(0, result.size());
            tx.commit();
            return null;
        });
    }

    @Test
    public void testIndexOnMultipleProperties() {
        session.writeTransaction(tx -> {
            tx.run("CREATE INDEX FOR (n:Foo) ON (n.bar, n.foo)");
            tx.commit();
            return null;
        });

        String indexName = session.readTransaction(tx -> {
            String name = tx.run("SHOW INDEXES YIELD name, type WHERE type <> 'LOOKUP' RETURN name").single().get("name").asString();
            tx.commit();
            return name;
        });

        session.writeTransaction(tx -> {
            tx.run("CALL db.awaitIndex($indexName)", Collections.singletonMap("indexName", indexName));
            tx.commit();
            return null;
        });
        testResult(session, "CALL apoc.schema.nodes() YIELD name, label, properties, status, type, " +
                "failure, populationProgress, size, valuesSelectivity, userDescription " +
                "WHERE label <> '<any-labels>' " +
                "RETURN *", (result) -> {
            // Get the index info
            Map<String, Object> r = result.next();

            assertEquals(":Foo(bar,foo)", r.get("name"));
            assertEquals("ONLINE", r.get("status"));
            assertEquals("Foo", r.get("label"));
            assertEquals("INDEX", r.get("type"));
            assertTrue(((List<String>) r.get("properties")).contains("bar"));
            assertTrue(((List<String>) r.get("properties")).contains("foo"));

            assertTrue(!result.hasNext());
        });
        session.writeTransaction(tx -> {
            tx.run("DROP INDEX " + indexName);
            tx.commit();
            return null;
        });
    }

    @Test
    public void testPropertyExistenceConstraintOnNode() {
        session.writeTransaction(tx -> {
            tx.run("CREATE CONSTRAINT foobarConstraint FOR (bar:Bar) REQUIRE (bar.foobar) IS NOT NULL");
            return null;
        });

        testResult(session, "CALL apoc.schema.nodes() YIELD name, label, properties, status, type, " +
                "failure, populationProgress, size, valuesSelectivity, userDescription " +
                "WHERE label <> '<any-labels>' " +
                "RETURN *", (result) -> {
            Map<String, Object> r = result.next();

            assertEquals("Bar", r.get("label"));
            assertEquals("NODE_PROPERTY_EXISTENCE", r.get("type"));
            assertEquals(asList("foobar"), r.get("properties"));

            assertTrue(!result.hasNext());
        });
        session.writeTransaction(tx -> {
            tx.run("DROP CONSTRAINT foobarConstraint");
            tx.commit();
            return null;
        });
    }

    @Test
    public void testConstraintExistsOnRelationship() {
        session.writeTransaction(tx -> {
            tx.run("CREATE CONSTRAINT likedConstraint FOR ()-[like:LIKED]-() REQUIRE (like.day) IS NOT NULL");
            tx.commit();
            return null;
        });
        testResult(session, "RETURN apoc.schema.relationship.constraintExists('LIKED', ['day'])", (result) -> {
            Map<String, Object> r = result.next();
            assertEquals(true, r.entrySet().iterator().next().getValue());
        });
        session.writeTransaction(tx -> {
            tx.run("DROP CONSTRAINT likedConstraint");
            tx.commit();
            return null;
        });
    }

    @Test
    public void testSchemaRelationships() {
        session.writeTransaction(tx -> {
            tx.run("CREATE CONSTRAINT likedConstraint FOR ()-[like:LIKED]-() REQUIRE (like.day) IS NOT NULL");
            tx.commit();
            return null;
        });
        testResult(session, "CALL apoc.schema.relationships()", (result) -> {
            Map<String, Object> r = result.next();
            assertEquals("CONSTRAINT ON ()-[liked:LIKED]-() ASSERT (liked.day) IS NOT NULL", r.get("name"));
            assertEquals("RELATIONSHIP_PROPERTY_EXISTENCE", r.get("type"));
            assertEquals(asList("day"), r.get("properties"));
            assertEquals(StringUtils.EMPTY, r.get("status"));
            assertFalse(result.hasNext());
        });
        session.writeTransaction(tx -> {
            tx.run("DROP CONSTRAINT likedConstraint");
            tx.commit();
            return null;
        });
    }
    
    @Test
    public void testSchemaNodeWithRelationshipsConstraintsAndViceVersa() {
        session.writeTransaction(tx -> {
            tx.run("CREATE CONSTRAINT ON ()-[like:LIKED]-() ASSERT exists(like.day)");
            tx.run("CREATE CONSTRAINT ON (bar:Bar) ASSERT exists(bar.foobar)");
            tx.commit();
            return null;
        });
        testResult(session, "CALL apoc.schema.relationships() YIELD name, type, properties, status " +
                "WHERE type <> '<any-types>' " +
                "RETURN *", (result) -> {
            Map<String, Object> r = result.next();
            assertEquals("CONSTRAINT ON ()-[liked:LIKED]-() ASSERT exists(liked.day)", r.get("name"));
            assertEquals("RELATIONSHIP_PROPERTY_EXISTENCE", r.get("type"));
            assertEquals(asList("day"), r.get("properties"));
            assertEquals(StringUtils.EMPTY, r.get("status"));
            assertFalse(result.hasNext());
        });
        testResult(session, "CALL apoc.schema.nodes()", (result) -> {
            Map<String, Object> r = result.next();
            assertEquals("Bar", r.get("label"));
            assertEquals("NODE_PROPERTY_EXISTENCE", r.get("type"));
            assertEquals(asList("foobar"), r.get("properties"));
            assertFalse(result.hasNext());
        });
        
        session.writeTransaction(tx -> {
            tx.run("DROP CONSTRAINT ON ()-[like:LIKED]-() ASSERT exists(like.day)");
            tx.run("DROP CONSTRAINT ON (bar:Bar) ASSERT exists(bar.foobar)");
            tx.commit();
            return null;
        });
    }

    private static void assertMatchesAll( List<String> expectedCreateStatements, List<String> actualCreateStatements )
    {
        for ( String expectedCreateStatement : expectedCreateStatements )
        {
            boolean foundStatement = false;
            int foundIndex = -1;
            for ( int i = 0; i < actualCreateStatements.size(); i++ )
            {
                if ( actualCreateStatements.get( i ).contains( expectedCreateStatement ) )
                {
                    foundStatement = true;
                    foundIndex = i;
                }
            }
            assertTrue( foundStatement );
            actualCreateStatements.remove( foundIndex );
        }
    }

    private List<String> expectedKeys(String... keys){
        return asList(keys);
    }
}
