package apoc.periodic;

import apoc.create.Create;
import apoc.load.Jdbc;
import apoc.nlp.gcp.GCPProcedures;
import apoc.nodes.NodesExtended;
import apoc.util.TestUtil;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testCallCount;
import static org.junit.Assert.assertEquals;
import static org.neo4j.test.assertion.Assert.assertEventually;

public class PeriodicExtendedTest {

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();

    @Before
    public void initDb() {
        TestUtil.registerProcedure(db, Periodic.class, NodesExtended.class, GCPProcedures.class, Create.class, PeriodicExtended.class, Jdbc.class);
    }

    @Test
    public void testSubmitSchema() {
        testCall(db, "CALL apoc.periodic.submitSchema('subSchema','CREATE INDEX periodicIdx FOR (n:Bar) ON (n.first_name, n.last_name)')",
                (row) -> {
                    assertEquals("subSchema", row.get("name"));
                    assertEquals(false, row.get("done"));
                });

        assertEventually(() -> db.executeTransactionally("SHOW INDEXES YIELD name WHERE name = 'periodicIdx' RETURN count(*) AS count",
                        Collections.emptyMap(),
                        (res) -> res.<Long>columnAs("count").next()),
                val -> val == 1L, 15L, TimeUnit.SECONDS);

        testCall(db, "CALL apoc.periodic.list()", (row) -> {
            assertEquals("subSchema", row.get("name"));
            assertEquals(true, row.get("done"));
        });
    }

    @Test
    public void testSubmitSchemaWithWriteOperation() {
        testCall(db, "CALL apoc.periodic.submitSchema('subSchema','CREATE (:SchemaLabel)')",
                (row) -> {
                    assertEquals("subSchema", row.get("name"));
                    assertEquals(false, row.get("done"));
                });

        assertEventually(() -> db.executeTransactionally("MATCH (n:SchemaLabel) RETURN count(n) AS count",
                        Collections.emptyMap(),
                        (res) -> res.<Long>columnAs("count").next()),
                val -> val == 1L, 15L, TimeUnit.SECONDS);

        testCall(db, "CALL apoc.periodic.list()", (row) -> {
            assertEquals("subSchema", row.get("name"));
            assertEquals(true, row.get("done"));
        });
    }

    @Test
    public void testRebindWithNlpWriteProcedure() {
        // use case: https://community.neo4j.com/t5/neo4j-graph-platform/use-of-apoc-periodic-iterate-with-apoc-nlp-gcp-classify-graph/m-p/56846#M33854
        final String iterate = "MATCH (node:Article) RETURN node";
        final String actionRebind = "CALL apoc.nlp.gcp.classify.graph(apoc.node.rebind(node), $nlpConf) YIELD graph RETURN null";
        testRebindCommon(iterate, actionRebind, 2, this::assertNoErrors);

        // "manual" rebind, i.e. "return id(node) as id" in iterate query, and "match .. where id(n)=id" in action query
        final String iterateId = "MATCH (node:Article) RETURN id(node) AS id";
        final String actionId = "MATCH (node) WHERE id(node) = id CALL apoc.nlp.gcp.classify.graph(node, $nlpConf) YIELD graph RETURN null";
        testRebindCommon(iterateId, actionId, 2, this::assertNoErrors);
    }

    @Test
    public void testRebindWithMapIterationAndCreateRelationshipProcedure() {
        final String iterate = "MATCH (art:Article) RETURN {key: art, key2: 'another'} as map";
        final String action = "CREATE (node:Category) with map.key as art, node call apoc.create.relationship(art, 'CATEGORY', {b: 1}, node) yield rel return rel";

        final String actionRebind = "WITH apoc.any.rebind(map) AS map " + action;
        testRebindCommon(iterate, actionRebind, 1, this::assertNoErrors);
    }

    private void assertNoErrors(Map<String, Object> r) {
        assertEquals(Collections.emptyMap(), r.get("errorMessages"));
    }

    private void testRebindCommon(String iterate, String action, int expected, Consumer<Map<String, Object>> assertions) {
        final Map<String, Object> nlpConf = Map.of("key", "myKey", "nodeProperty", "content", "write", true, "unsupportedDummyClient", true);
        final Map<String, Object> config = Map.of("params", Map.of("nlpConf", nlpConf));

        db.executeTransactionally("CREATE (:Article {content: 'contentBody'})");
        testCall(db,"CALL apoc.periodic.iterate($iterate, $action, $config)",
                Map.of( "iterate" , iterate, "action", action, "config", config),
                assertions);

        testCallCount(db, "MATCH p=(:Category)<-[:CATEGORY]-(:Article) RETURN p", expected);
        db.executeTransactionally("MATCH (n) DETACH DELETE n");
    }
}
