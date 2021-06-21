package apoc.export.json;

import apoc.ApocSettings;
import apoc.schema.Schemas;
import apoc.util.JsonUtil;
import apoc.util.TestUtil;
import junit.framework.TestCase;
import org.apache.commons.lang.exception.ExceptionUtils;
import junit.framework.TestCase;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.spatial.Point;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.Values;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

import static apoc.export.json.JsonImporter.CREATE_CONSTRAINT_TEMPLATE;
import static apoc.export.json.JsonImporter.MISSING_CONSTRAINT_ERROR_MSG;
import static apoc.util.MapUtil.map;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.driver.internal.util.Iterables.count;
import static java.lang.String.format;
import static org.neo4j.driver.internal.util.Iterables.count;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


public class ImportJsonTest {

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
        TestUtil.registerProcedure(db, ImportJson.class, Schemas.class);
    }

    @Test
    public void shouldImportAllJson() throws Exception {
        db.executeTransactionally("CREATE CONSTRAINT ON (n:User) assert n.neo4jImportId IS UNIQUE");
        
        // given
        String filename = "all.json";

        // when
        TestUtil.testCall(db, "CALL apoc.import.json($file, null)",
                map("file", filename),
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

                        Assert.assertEquals(9, props.size());
                        Assert.assertEquals("wgs-84", props.get("place.crs"));
                        Assert.assertEquals(33.46789D, props.get("place.latitude"));
                        Assert.assertEquals(13.1D, props.get("place.longitude"));
                        Assert.assertFalse(props.containsKey("place"));
                    }
                }
        );
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
        );
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
                    try (Transaction tx = db.beginTx()) {
                        Node node = tx.execute("MATCH (n) WHERE n.neo4jImportId = '5016999' RETURN n")
                                .<Node>columnAs("n")
                                .next();
                        Assert.assertNotNull("node should be not null", node);
                        final double[] actual = (double[]) node.getProperty("bbox");
                        Assert.assertArrayEquals(expected, actual, 0.05D);
                    }
                }
        );
    }

    @Test
    public void shouldTerminateImportWhenTransactionIsTimedOut() throws Exception {
        restartDb(Duration.ofMillis(1));

        createConstraints("neo4jImportId", List.of("Stream", "User", "Game", "Team", "Language"));

        String filename = "big.json";

        try {
            TestUtil.testCall(db, "CALL apoc.import.json($file)",
                    map("file", filename), (r) -> fail("Should fail due to timeout exception"));
        } catch (Exception e) {
            String expected = "The transaction has been terminated. Retry your operation in a new transaction, and you should see a successful result.";
            assertTrue(e.getMessage().contains(expected));
        }
        
        try (Transaction tx = db.beginTx()) {
            // check that not all nodes have been imported
            assertTrue(count(tx.getAllNodes()) < 1042);
        }

        restartDb(Duration.ZERO);
    }
    
    @Test
    public void shouldFailBecauseOfMissingConstraintException() {
        String customId = "customId";
        createConstraints(customId, List.of("Stream", "Game"));
        assertNoRel();

        String filename = "big.json";
        try {
            TestUtil.testCall(db, "CALL apoc.import.json($file, {importIdName: $importIdName})",
                    map("file", filename, "importIdName", customId),
                    (r) -> fail("Should fail due to missing constraint")
            );
        } catch (RuntimeException e) {
            Throwable except = ExceptionUtils.getRootCause(e);
            TestCase.assertTrue(except instanceof RuntimeException);
            String expectedMsg = MISSING_CONSTRAINT_ERROR_MSG 
                    + format(CREATE_CONSTRAINT_TEMPLATE, "User", customId) + "\n"
                    + format(CREATE_CONSTRAINT_TEMPLATE, "Language", customId);
            assertEquals(expectedMsg, except.getMessage());
        }

        // check that no rels created after constraint exception
        assertNoRel();
    }

    private void createConstraints(String customId, List<String> labels) {
        labels.forEach(label -> db.executeTransactionally(format(CREATE_CONSTRAINT_TEMPLATE, label, customId)));
    }

    private void assertNoRel() {
        try (Transaction tx = db.beginTx()) {
            assertEquals(0L, count(tx.getAllRelationships()));
        }
    }

    private void restartDb(Duration value) throws Exception {
        db.shutdown();
        db.withSetting(GraphDatabaseSettings.transaction_timeout, value);
        db.restartDatabase();
        setUp();
    }
}
