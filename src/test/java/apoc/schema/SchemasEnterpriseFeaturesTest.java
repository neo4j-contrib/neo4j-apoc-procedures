package apoc.schema;

import apoc.util.Neo4jContainerExtension;
import apoc.util.TestUtil;
import org.apache.commons.lang3.StringUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;

import java.util.List;
import java.util.Map;

import static apoc.util.TestContainerUtil.*;
import static java.util.Arrays.asList;
import static org.junit.Assert.*;

/**
 * @author as
 * @since 12.02.19
 */
public class SchemasEnterpriseFeaturesTest {

    private static Neo4jContainerExtension neo4jContainer;
    private static Session session;

    @BeforeClass
    public static void beforeAll() {
        executeGradleTasks("clean", "shadow");
        neo4jContainer = createEnterpriseDB(!TestUtil.isCI());
        neo4jContainer.start();
        session = neo4jContainer.getSession();
    }

    @AfterClass
    public static void afterAll() {
        session.close();
        neo4jContainer.close();
        cleanBuild();
    }

    @Test
    public void testDropNodeKeyConstraintAndCreateNodeKeyConstraintWhenUsingDropExisting() {
        session.writeTransaction(tx -> {
            tx.run("CREATE CONSTRAINT ON (f:Foo) ASSERT (f.bar,f.foo) IS NODE KEY");
            tx.success();
            return null;
        });
        testResult(session, "CALL apoc.schema.assert(null,{Foo:[['bar','foo']]})", (result) -> {
            Map<String, Object> r = result.next();
            assertEquals("Foo", r.get("label"));
            assertEquals(expectedKeys("bar","foo"), r.get("keys"));
            assertEquals(true, r.get("unique"));
            assertEquals("DROPPED", r.get("action"));

            r = result.next();
            assertEquals("Foo", r.get("label"));
            assertEquals(expectedKeys("bar", "foo"), r.get("keys"));
            assertEquals(true, r.get("unique"));
            assertEquals("CREATED", r.get("action"));
        });

        session.readTransaction(tx -> {
            List<Record> result = tx.run("CALL db.constraints").list();
            assertEquals(1, result.size());
            tx.run("DROP CONSTRAINT ON (f:Foo) ASSERT (f.bar,f.foo) IS NODE KEY").list();
            tx.success();
            return null;
        });
    }

    @Test
    public void testDropSchemaWithNodeKeyConstraintWhenUsingDropExisting() {
        session.writeTransaction(tx -> {
            tx.run("CREATE CONSTRAINT ON (f:Foo) ASSERT (f.foo, f.bar) IS NODE KEY");
            tx.success();
            return null;
        });
        testCall(session, "CALL apoc.schema.assert(null,null)", (r) -> {
            assertEquals("Foo", r.get("label"));
            assertEquals(expectedKeys("foo", "bar"), r.get("keys"));
            assertEquals(true, r.get("unique"));
            assertEquals("DROPPED", r.get("action"));
        });

        session.readTransaction(tx -> {
            List<Record> result = tx.run("CALL db.constraints").list();
            assertEquals(0, result.size());
            tx.success();
            return null;
        });
    }

    @Test
    public void testDropConstraintExistsPropertyNode() {
        session.writeTransaction(tx -> {
            tx.run("CREATE CONSTRAINT ON (m:Movie) ASSERT exists(m.title)");
            tx.success();
            return null;
        });
        testCall(session, "CALL apoc.schema.assert({},{})", (r) -> {
            assertEquals("Movie", r.get("label"));
            assertEquals(expectedKeys("title"), r.get("keys"));
            assertTrue("should be unique", (boolean) r.get("unique"));
            assertEquals("DROPPED", r.get("action"));
        });

        session.readTransaction(tx -> {
            List<Record> result = tx.run("CALL db.constraints").list();
            assertEquals(0, result.size());
            tx.success();
            return null;
        });
    }

    @Test
    public void testDropConstraintExistsPropertyRelationship() {
        session.writeTransaction(tx -> {
            tx.run("CREATE CONSTRAINT ON ()-[acted:Acted]->() ASSERT exists(acted.since)");
            tx.success();
            return null;
        });

        testCall(session, "CALL apoc.schema.assert({},{})", (r) -> {
            assertEquals("Acted", r.get("label"));
            assertEquals(expectedKeys("since"), r.get("keys"));
            assertTrue("should be unique", (boolean) r.get("unique"));
            assertEquals("DROPPED", r.get("action"));
        });

        session.readTransaction(tx -> {
            List<Record> result = tx.run("CALL db.constraints").list();
            assertEquals(0, result.size());
            tx.success();
            return null;
        });
    }

    @Test
    public void testIndexOnMultipleProperties() {
        session.writeTransaction(tx -> {
            tx.run("CREATE INDEX ON :Foo(bar, foo)");
            tx.success();
            return null;
        });
        session.writeTransaction(tx -> {
            tx.run("CALL db.awaitIndex(':Foo(bar, foo)')");
            tx.success();
            return null;
        });
        testResult(session, "CALL apoc.schema.nodes()", (result) -> {
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
            tx.run("DROP INDEX ON :Foo(bar, foo)");
            tx.success();
            return null;
        });
    }

    @Test
    public void testPropertyExistenceConstraintOnNode() {
        session.writeTransaction(tx -> {
            tx.run("CREATE CONSTRAINT ON (bar:Bar) ASSERT exists(bar.foobar)");
            tx.success();
            return null;
        });
        testResult(session, "CALL apoc.schema.nodes()", (result) -> {
            Map<String, Object> r = result.next();

            assertEquals("Bar", r.get("label"));
            assertEquals("NODE_PROPERTY_EXISTENCE", r.get("type"));
            assertEquals(asList("foobar"), r.get("properties"));

            assertTrue(!result.hasNext());
        });
        session.writeTransaction(tx -> {
            tx.run("DROP CONSTRAINT ON (bar:Bar) ASSERT exists(bar.foobar)");
            tx.success();
            return null;
        });
    }

    @Test
    public void testConstraintExistsOnRelationship() {
        session.writeTransaction(tx -> {
            tx.run("CREATE CONSTRAINT ON ()-[like:LIKED]-() ASSERT exists(like.day)");
            tx.success();
            return null;
        });
        testResult(session, "RETURN apoc.schema.relationship.constraintExists('LIKED', ['day'])", (result) -> {
            Map<String, Object> r = result.next();
            assertEquals(true, r.entrySet().iterator().next().getValue());
        });
        session.writeTransaction(tx -> {
            tx.run("DROP CONSTRAINT ON ()-[like:LIKED]-() ASSERT exists(like.day)");
            tx.success();
            return null;
        });
    }

    @Test
    public void testSchemaRelationships() {
        session.writeTransaction(tx -> {
            tx.run("CREATE CONSTRAINT ON ()-[like:LIKED]-() ASSERT exists(like.day)");
            tx.success();
            return null;
        });
        testResult(session, "CALL apoc.schema.relationships()", (result) -> {
            Map<String, Object> r = result.next();
            assertEquals("CONSTRAINT ON ()-[liked:LIKED]-() ASSERT exists(liked.day)", r.get("name"));
            assertEquals("RELATIONSHIP_PROPERTY_EXISTENCE", r.get("type"));
            assertEquals(asList("day"), r.get("properties"));
            assertEquals(StringUtils.EMPTY, r.get("status"));
            assertFalse(result.hasNext());
        });
        session.writeTransaction(tx -> {
            tx.run("DROP CONSTRAINT ON ()-[like:LIKED]-() ASSERT exists(like.day)");
            tx.success();
            return null;
        });
    }

    private List<String> expectedKeys(String... keys){
        return asList(keys);
    }
}
