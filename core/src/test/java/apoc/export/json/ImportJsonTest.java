package apoc.export.json;

import apoc.ApocSettings;
import apoc.schema.Schemas;
import apoc.util.CompressionAlgo;
import apoc.util.JsonUtil;
import apoc.util.TestUtil;
import apoc.util.Util;
import junit.framework.TestCase;
import org.apache.commons.lang.exception.ExceptionUtils;
import apoc.util.Utils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.spatial.Point;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.Values;

import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static apoc.export.json.ImportJsonConfig.WILDCARD_PROPS;
import static apoc.export.json.JsonImporter.MISSING_CONSTRAINT_ERROR_MSG;
import static apoc.util.BinaryTestUtil.fileToBinary;
import static apoc.util.CompressionConfig.COMPRESSION;
import static apoc.util.MapUtil.map;
import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.test.assertion.Assert.assertEventually;


public class ImportJsonTest {

    private static final long NODES_BIG_JSON = 16L;
    private static final long RELS_BIG_JSON = 4L;
    private static File directory = new File("../docs/asciidoc/modules/ROOT/examples/data/exportJSON");

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(GraphDatabaseSettings.load_csv_file_url_root, directory.getCanonicalFile().toPath())
            .withSetting(GraphDatabaseSettings.procedure_unrestricted, List.of("apoc.*"))
            .withSetting(ApocSettings.apoc_import_file_enabled, true);

    public ImportJsonTest() throws IOException {
    }

    @Before
    public void setUp() throws Exception {
        TestUtil.registerProcedure(db, ImportJson.class, Schemas.class, Utils.class);
    }
    
    @Test
    public void shouldImportAllJsonWithoutImportId() {
        shouldImportAllCommon(map("cleanup", true), 8, 0L);
    }

    @Test
    public void shouldImportAllJson() throws Exception {
        shouldImportAllCommon(Collections.emptyMap(), 9, 1L);
    }

    private void shouldImportAllCommon(Map<String, Object> config, int expectedPropSize, long relCount) {
        db.executeTransactionally("CREATE CONSTRAINT ON (n:User) assert n.neo4jImportId IS UNIQUE");

        // given
        String filename = "all.json";

        // when
        TestUtil.testCall(db, "CALL apoc.import.json($file, $config)",
                map("file", filename, "config", config),
                (r) -> assertionsAllJsonProgressInfo(r, false)
        );

        assertionsAllJsonDbResult(expectedPropSize, relCount);
    }

    @Test
    public void shouldImportAllJsonWithPropertyMappings() throws Exception {
        db.executeTransactionally("CREATE CONSTRAINT ON (n:User) assert n.neo4jImportId IS UNIQUE");
        // given
        String filename = "all.json";

        // when
        TestUtil.testCall(db, "CALL apoc.import.json($file, $config)",
                map("file", filename, "config",
                        map("nodePropertyMappings", map("User", map("place", "Point", "born", "LocalDateTime")),
                        "relPropertyMappings", map("KNOWS", map("bffSince", "Duration")), "unwindBatchSize", 1, "txBatchSize", 1)),
                (r) -> {
                    // then
                    Assert.assertEquals("all.json", r.get("file"));
                    Assert.assertEquals("file", r.get("source"));
                    Assert.assertEquals("json", r.get("format"));
                    Assert.assertEquals(3L, r.get("nodes"));
                    Assert.assertEquals(1L, r.get("relationships"));
                    Assert.assertEquals(15L, r.get("properties"));
                    Assert.assertEquals(4L, r.get("rows"));
                    Assert.assertEquals(true, r.get("done"));

                }
        );
        
        try(Transaction tx = db.beginTx()) {
            final long countNodes = tx.execute("MATCH (n:User) RETURN count(n) AS count")
                    .<Long>columnAs("count")
                    .next();
            Assert.assertEquals(3L, countNodes);

            final long countRels = tx.execute("MATCH ()-[r:KNOWS]->() RETURN count(r) AS count")
                    .<Long>columnAs("count")
                    .next();
            Assert.assertEquals(1L, countRels);

            final Map<String, Object> props = tx.execute("MATCH (n:User {name: 'Adam'}) RETURN n")
                    .<Node>columnAs("n")
                    .next()
                    .getAllProperties();
            Assert.assertEquals(7, props.size());
            Assert.assertTrue(props.get("place") instanceof PointValue);
            PointValue point = (PointValue) props.get("place");
            final PointValue pointValue = Values.pointValue(CoordinateReferenceSystem.WGS84, 33.46789D, 13.1D);
            Assert.assertTrue(point.equals((Point) pointValue));
            Assert.assertTrue(props.get("born") instanceof LocalDateTime);

            Relationship rel = tx.execute("MATCH ()-[r:KNOWS]->() RETURN r")
                    .<Relationship>columnAs("r")
                    .next();
            Assert.assertTrue(rel.getProperty("bffSince") instanceof DurationValue);
            Assert.assertEquals("P5M1DT12H", rel.getProperty("bffSince").toString());
        }
    }

    @Test
    public void shouldImportNodesWithoutLabels() throws Exception {
        // given
        String filename = "nodes_without_labels.json";
        Map<String, Object> jsonMap = JsonUtil.OBJECT_MAPPER
                                .readValue(new File(directory, filename), Map.class);
        Map<String, Object> properties = (Map<String, Object>) jsonMap.get("properties");
        List<Double> bbox = (List<Double>) properties.get("bbox");
        final double[] expected = bbox.stream().mapToDouble(Double::doubleValue).toArray();

        // when
        TestUtil.testCall(db, "CALL apoc.import.json($file)",
                map("file", filename),
                (r) -> {
                    // then
                    Assert.assertEquals(filename, r.get("file"));
                    Assert.assertEquals("file", r.get("source"));
                    Assert.assertEquals("json", r.get("format"));
                    Assert.assertEquals(1L, r.get("nodes"));
                    Assert.assertEquals(0L, r.get("relationships"));
                    Assert.assertEquals(2L, r.get("properties"));
                    Assert.assertEquals(1L, r.get("rows"));
                    Assert.assertEquals(true, r.get("done"));
                }
        );

        try (Transaction tx = db.beginTx()) {
            Node node = tx.execute("MATCH (n) WHERE n.neo4jImportId = '5016999' RETURN n")
                    .<Node>columnAs("n")
                    .next();
            Assert.assertNotNull("node should be not null", node);
            final double[] actual = (double[]) node.getProperty("bbox");
            Assert.assertArrayEquals(expected, actual, 0.05D);
        }
    }
    
    @Test
    public void shouldTerminateImportWhenTransactionIsTimedOut() {

        createConstraints(List.of("Stream", "User", "Game", "Team", "Language"));

        String filename = "https://devrel-data-science.s3.us-east-2.amazonaws.com/twitch_all.json";

        final String query = "CALL apoc.import.json($file)";
        new Thread(() -> db.executeTransactionally(query,  map("file", filename))).start();

        // waiting for 'apoc.import.json() query to cancel when it is found
        final String transactionId = TestUtil.singleResultFirstColumn(db, 
                "SHOW TRANSACTIONS YIELD currentQuery, transactionId WHERE currentQuery = $query RETURN transactionId",
                map("query", query));
        
        assertEventually(() -> db.executeTransactionally("TERMINATE TRANSACTION $transactionId",
                map("transactionId", transactionId),
                result -> {
                    final ResourceIterator<String> msgIterator = result.columnAs("message");
                    return msgIterator.hasNext() && msgIterator.next().equals("Transaction terminated.");
                }), (value) -> value, 10L, TimeUnit.SECONDS);

        // checking for query cancellation
        assertEventually(() -> {
            final String transactionListCommand = "SHOW TRANSACTIONS";
            return db.executeTransactionally(transactionListCommand,
                    map("query", query),
                    result -> {
                        final ResourceIterator<String> queryIterator = result.columnAs("currentQuery");
                        final String first = queryIterator.next();
                        return first.equals(transactionListCommand) && !queryIterator.hasNext();
                    } );
        }, (value) -> value, 10L, TimeUnit.SECONDS);
    }

    @Test
    public void shouldImportAllNodesAndRelsWithFilterAll() {
        createConstraints(List.of("FirstLabel", "Stream", "User", "Game", "Team", "Language", "$User", "$Stream"));
        assertEntities(0L, 0L);

        String filename = "multiLabels.json";

        TestUtil.testCall(db, "CALL apoc.import.json($file, $config)",
                map("file", filename, 
                        "config", map("nodePropFilter", map(WILDCARD_PROPS, List.of("name")),
                                "relPropFilter", map(WILDCARD_PROPS, List.of("bffSince")))), 
                r -> {
                    assertEquals(NODES_BIG_JSON, r.get("nodes"));
                    assertEquals(RELS_BIG_JSON, r.get("relationships"));
                });

        final Consumer<Node> nodeConsumer = node -> assertFalse(node.hasProperty("name"));
        final Consumer<Relationship> relConsumer = rel -> assertFalse(rel.hasProperty("bffSince"));
        assertEntities(NODES_BIG_JSON, RELS_BIG_JSON, nodeConsumer, relConsumer);
    }
    
    @Test
    public void shouldImportAllNodesAndRelsWithLabelAndRelTypeFilter() {
        createConstraints(List.of("FirstLabel", "Stream", "User", "Game", "Team", "Language", "$User", "$Stream"));
        assertEntities(0L, 0L);

        String filename = "multiLabels.json";

        TestUtil.testCall(db, "CALL apoc.import.json($file, $config)",
                map("file", filename, 
                        "config", map("nodePropFilter", map("Stream", List.of("name"), 
                                        "$User", List.of("total_view_count", "url"), 
                                        "$Stream", List.of("name", "id")), 
                                "relPropFilter", map("REL_GAME_TO_LANG", List.of("since")))
                ), r -> {
                    assertEquals(NODES_BIG_JSON, r.get("nodes"));
                    assertEquals(RELS_BIG_JSON, r.get("relationships"));
                });

        final Consumer<Node> nodeConsumer = node -> {
            if (node.hasLabel(Label.label("Stream"))) {
                final Set<String> actual = Iterables.asSet(node.getPropertyKeys());
                assertEquals(Set.of("createdAt", "followers", "description", "id", "total_view_count", "url", "neo4jImportId"), actual);
            }
            if (node.hasLabel(Label.label("$User"))) {
                final Set<String> actual = Iterables.asSet(node.getPropertyKeys());
                assertEquals(Set.of("createdAt", "followers", "description", "neo4jImportId"), actual);
            }
        };
        
        final Consumer<Relationship> relConsumer = rel -> {
            final boolean hasTypeRelGameToLang = rel.getType().name().equals("REL_GAME_TO_LANG");
            assertEquals(!hasTypeRelGameToLang, rel.hasProperty("since"));
            assertTrue(rel.hasProperty("bffSince"));
        };

        assertEntities(NODES_BIG_JSON, RELS_BIG_JSON, nodeConsumer, relConsumer);
    }
    
    @Test
    public void shouldImportAllNodesAndRels() {
        createConstraints(List.of("FirstLabel", "Stream", "User", "Game", "Team", "Language", "$User", "$Stream"));
        assertEntities(0L, 0L);

        String filename = "multiLabels.json";
        
        TestUtil.testCall(db, "CALL apoc.import.json($file)", 
                map("file", filename), (r) -> {
                    assertEquals(NODES_BIG_JSON, r.get("nodes"));
                    assertEquals(RELS_BIG_JSON, r.get("relationships"));
        });
        
        assertEntities(NODES_BIG_JSON, RELS_BIG_JSON);
    }

    @Test
    public void shouldFailBecauseOfMissingUniquenessConstraintException() {
        db.executeTransactionally("CREATE CONSTRAINT FOR (n:User) REQUIRE (n.neo4jImportId, n.name) IS UNIQUE;");
        assertEntities(0L, 0L);

        String filename = "all.json";
        try {
            TestUtil.testCall(db, "CALL apoc.import.json($file, {})",
                    map("file", filename),
                    (r) -> fail("Should fail due to missing constraint")
            );
        } catch (Exception e) {
            String expectedMsg = format(MISSING_CONSTRAINT_ERROR_MSG, "User", "neo4jImportId");
            assertRootMessage(expectedMsg, e);
        }
    }
    
    @Test
    public void shouldFailBecauseOfMissingSecondConstraintException() {
        String customId = "customId";
        createConstraints(List.of("FirstLabel", "Stream", "Game", "$User"), customId);
        assertEntities(0L, 0L);

        String filename = "multiLabels.json";
        try {
            TestUtil.testCall(db, "CALL apoc.import.json($file, {importIdName: $importIdName})",
                    map("file", filename, "importIdName", customId),
                    (r) -> fail("Should fail due to missing constraint")
            );
        } catch (Exception e) {
            String expectedMsg = format(MISSING_CONSTRAINT_ERROR_MSG, "User", customId);
            assertRootMessage(expectedMsg, e);
        }

        // check that only 1st node created after constraint exception
        assertEntities(1L, 0L);
    }

    private void assertRootMessage(String expectedMsg, Exception e) {
        Throwable except = ExceptionUtils.getRootCause(e);
        TestCase.assertTrue(except instanceof RuntimeException);
        assertEquals(expectedMsg, except.getMessage());
    }

    private void createConstraints(List<String> labels, String customId) {
        labels.forEach(
                label -> db.executeTransactionally(format("CREATE CONSTRAINT ON (n:%s) assert n.%s IS UNIQUE;", Util.quote(label), customId)));
    }

    private void createConstraints(List<String> labels) {
        createConstraints(labels, "neo4jImportId");
    }

    private void assertEntities(long expectedNodes, long expectedRels) {
        assertEntities(expectedNodes, expectedRels, null, null);
    }

    private void assertEntities(long expectedNodes, long expectedRels, Consumer<Node> nodeConsumer, Consumer<Relationship> relConsumer) {
        try (Transaction tx = db.beginTx()) {
            final List<Node> nodeList = Iterables.asList(tx.getAllNodes());
            final List<Relationship> relList = Iterables.asList((tx.getAllRelationships()));
            assertEquals(expectedNodes, nodeList.size());
            assertEquals(expectedRels, relList.size());
            if (nodeConsumer != null) {
                nodeList.forEach(nodeConsumer);
            }
            if (relConsumer != null) {
                relList.forEach(relConsumer);
            }
        }
    }

    @Test
    public void shouldImportAllJsonFromBinary()  {
        db.executeTransactionally("CREATE CONSTRAINT ON (n:User) assert n.neo4jImportId IS UNIQUE");
        
        TestUtil.testCall(db, "CALL apoc.import.json($file, $config)",
                map("config", map(COMPRESSION, CompressionAlgo.DEFLATE.name()),
                        "file", fileToBinary(new File(directory, "all.json"), CompressionAlgo.DEFLATE.name())),
                (r) -> assertionsAllJsonProgressInfo(r, true));

        assertionsAllJsonDbResult();
    }

    private void assertionsAllJsonProgressInfo(Map<String, Object> r, boolean isBinary) {
        // then
        Assert.assertEquals(isBinary ? null : "all.json", r.get("file"));
        Assert.assertEquals(isBinary ? "binary" : "file", r.get("source"));
        Assert.assertEquals("json", r.get("format"));
        Assert.assertEquals(3L, r.get("nodes"));
        Assert.assertEquals(1L, r.get("relationships"));
        Assert.assertEquals(15L, r.get("properties"));
        Assert.assertEquals(4L, r.get("rows"));
        Assert.assertEquals(true, r.get("done"));
    }

    private void assertionsAllJsonDbResult() {
        assertionsAllJsonDbResult(9, 1L);
    }
    
    private void assertionsAllJsonDbResult(int expectedPropSize, long relCount) {
        try(Transaction tx = db.beginTx()) {
            final long countNodes = tx.execute("MATCH (n:User) RETURN count(n) AS count")
                    .<Long>columnAs("count")
                    .next();
            Assert.assertEquals(3L, countNodes);

            final long countRels = tx.execute("MATCH ()-[r:KNOWS]->() RETURN count(r) AS count")
                    .<Long>columnAs("count")
                    .next();
            Assert.assertEquals(relCount, countRels);

            final Map<String, Object> props = tx.execute("MATCH (n:User {name: 'Adam'}) RETURN n")
                    .<Node>columnAs("n")
                    .next()
                    .getAllProperties();

            Assert.assertEquals(expectedPropSize, props.size());
            Assert.assertEquals("wgs-84", props.get("place.crs"));
            Assert.assertEquals(13.1D, props.get("place.latitude"));
            Assert.assertEquals(33.46789D, props.get("place.longitude"));
            Assert.assertFalse(props.containsKey("place"));
        }
    }
}
