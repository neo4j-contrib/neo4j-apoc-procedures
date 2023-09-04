package apoc.schemas;

import apoc.util.Neo4jContainerExtension;
import apoc.util.TestContainerUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.driver.Session;

import java.util.List;
import java.util.Map;

import static apoc.util.TestContainerUtil.createEnterpriseDB;
import static apoc.util.TestContainerUtil.testResult;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class SchemasExtendedEnterpriseFeatureTest {
    private static Neo4jContainerExtension neo4jContainer;
    private static Session session;

    @BeforeClass
    public static void beforeAll() {
        // We build the project, the artifact will be placed into ./build/libs
        neo4jContainer = createEnterpriseDB(List.of(TestContainerUtil.ApocPackage.FULL), true);
        neo4jContainer.start();
        session = neo4jContainer.getSession();
    }

    @AfterClass
    public static void afterAll() {
        session.close();
        neo4jContainer.close();
    }

    @Test
    public void testCompareIndexesAndConstraintsRelEnterprise() {
        session.writeTransaction(tx -> {
            tx.run("CREATE CONSTRAINT foo_bar FOR (f:Foo) REQUIRE (f.bar,f.foo) IS NODE KEY");
            tx.run("CREATE CONSTRAINT bar_foobar FOR (bar:Bar) REQUIRE bar.foobar IS NOT NULL");
            tx.run("CREATE CONSTRAINT another_one_cons FOR (n:Another) REQUIRE n.one IS NOT NULL");
            tx.run("CREATE CONSTRAINT another_two_cons FOR (n:Another) REQUIRE n.two IS NOT NULL");
            tx.run("CREATE CONSTRAINT like_day FOR ()-[like:LIKED]-() REQUIRE like.day IS NOT NULL");
            tx.commit();
            return null;
        });

        session.writeTransaction(tx -> {
            tx.run("CREATE RANGE INDEX person_surname FOR (n:Person) ON (n.surname)");
            tx.run("CREATE POINT INDEX another_one_idx FOR (n:Another) ON (n.one)");
            tx.run("CREATE TEXT INDEX rel_index_name FOR ()-[r:KNOWS]-() ON (r.since)");
            tx.commit();
            return null;
        });

        session.writeTransaction(tx -> {
            tx.run("CREATE FULLTEXT INDEX fullIdxNode FOR (n:Person|Another) ON EACH [n.weightProp]");
            tx.run("CREATE FULLTEXT INDEX fullIdxRel FOR ()-[r:TYPE_1|TYPE_2]-() ON EACH [r.alpha, r.beta]");
            tx.commit();
            return null;
        });

        testResult(session, "CALL apoc.schema.node.compareIndexesAndConstraints()", (res) -> {
            Map<String, Object> row = res.next();
            assertEquals(Map.of(":<any-labels>()", emptyList()), row.get("onlyIdxProps"));
            assertEquals("<any-labels>", row.get("label"));
            assertEquals(emptyMap(), row.get("onlyConstraintsProps"));
            assertEquals(emptyList(), row.get("commonProps"));
            row = res.next();
            assertEquals("Another", row.get("label"));
            assertEquals(Map.of(":[Another, Person],(weightProp)", List.of("weightProp")), row.get("onlyIdxProps"));
            assertEquals(Map.of(":Another(two)", List.of("two")), row.get("onlyConstraintsProps"));
            assertEquals(List.of("one"), row.get("commonProps"));
            row = res.next();
            assertEquals("Bar", row.get("label"));
            assertEquals(emptyMap(), row.get("onlyIdxProps"));
            assertEquals(Map.of(":Bar(foobar)", List.of("foobar")), row.get("onlyConstraintsProps"));
            assertEquals(emptyList(), row.get("commonProps"));
            row = res.next();
            assertEquals("Foo", row.get("label"));
            assertEquals(emptyMap(), row.get("onlyIdxProps"));
            assertEquals(emptyMap(), row.get("onlyConstraintsProps"));
            assertEquals(List.of("bar", "foo"), row.get("commonProps"));
            row = res.next();
            assertEquals("Person", row.get("label"));
            assertEquals(Map.of(":[Another, Person],(weightProp)", List.of("weightProp"), ":Person(surname)", List.of("surname")), row.get("onlyIdxProps"));
            assertEquals(emptyMap(), row.get("onlyConstraintsProps"));
            assertEquals(emptyList(), row.get("commonProps"));
            assertFalse(res.hasNext());
        });

        testResult(session, "CALL apoc.schema.relationship.compareIndexesAndConstraints()", (res) -> {
            Map<String, Object> row = res.next();
            assertEquals("<any-types>", row.get("relationshipType"));
            assertEquals(Map.of(":<any-types>()", emptyList()), row.get("onlyIdxProps"));
            assertEquals(emptyMap(), row.get("onlyConstraintsProps"));
            assertEquals(emptyList(), row.get("commonProps"));
            row = res.next();
            assertEquals("KNOWS", row.get("relationshipType"));
            assertEquals(Map.of(":KNOWS(since)", List.of("since")), row.get("onlyIdxProps"));
            assertEquals(emptyMap(), row.get("onlyConstraintsProps"));
            assertEquals(emptyList(), row.get("commonProps"));
            row = res.next();
            assertEquals("LIKED", row.get("relationshipType"));
            assertEquals(emptyMap(), row.get("onlyIdxProps"));
            assertEquals(Map.of("CONSTRAINT ON ()-[liked:LIKED]-() ASSERT (liked.day) IS NOT NULL", List.of("day")), row.get("onlyConstraintsProps"));
            assertEquals(emptyList(), row.get("commonProps"));
            row = res.next();
            assertEquals("TYPE_1", row.get("relationshipType"));
            assertEquals(Map.of(":[TYPE_1, TYPE_2],(alpha,beta)", List.of("alpha", "beta")), row.get("onlyIdxProps"));
            assertEquals(emptyMap(), row.get("onlyConstraintsProps"));
            assertEquals(emptyList(), row.get("commonProps"));
            row = res.next();
            assertEquals("TYPE_2", row.get("relationshipType"));
            assertEquals(Map.of(":[TYPE_1, TYPE_2],(alpha,beta)", List.of("alpha", "beta")), row.get("onlyIdxProps"));
            assertEquals(emptyMap(), row.get("onlyConstraintsProps"));
            assertEquals(emptyList(), row.get("commonProps"));
            assertFalse(res.hasNext());
        });
    }

}
