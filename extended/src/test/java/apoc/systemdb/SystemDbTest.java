package apoc.systemdb;

import apoc.ApocConfig;
import apoc.ExtendedApocConfig;
import apoc.custom.CypherProcedures;
import apoc.cypher.CypherExtended;
import apoc.dv.DataVirtualizationCatalog;
import apoc.periodic.Periodic;
import apoc.systemdb.metadata.ExportMetadata;
import apoc.trigger.Trigger;
import apoc.util.MapUtil;
import apoc.util.TestUtil;
import apoc.util.collection.Iterables;
import apoc.util.collection.Iterators;
import apoc.uuid.Uuid;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import org.apache.commons.io.FileUtils;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.jupiter.api.AfterAll;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static apoc.ApocConfig.apocConfig;
import static apoc.systemdb.SystemDbConfig.FEATURES_KEY;
import static apoc.systemdb.SystemDbConfig.FILENAME_KEY;
import static org.junit.Assert.assertEquals;

public class SystemDbTest {
    private static File directory = new File("target/import");

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(GraphDatabaseSettings.load_csv_file_url_root, directory.toPath().toAbsolutePath());
    
    static { //noinspection ResultOfMethodCallIgnored
        directory.mkdirs();
    }

    @Before
    public void setUp() throws Exception {
        apocConfig().setProperty(ApocConfig.APOC_IMPORT_FILE_ENABLED, true);
        apocConfig().setProperty(ApocConfig.APOC_EXPORT_FILE_ENABLED, true);
        apocConfig().setProperty(ExtendedApocConfig.APOC_UUID_ENABLED, true);
        apocConfig().setProperty(ApocConfig.APOC_TRIGGER_ENABLED, true);
        TestUtil.registerProcedure(db, SystemDb.class, Trigger.class, CypherProcedures.class, Uuid.class, Periodic.class, DataVirtualizationCatalog.class, CypherExtended.class);
    }

    @AfterAll
    public void tearDown() {
        db.shutdown();
    }

    @Test
    public void testGetGraph() throws Exception {
        TestUtil.testResult(db, "CALL apoc.systemdb.graph() YIELD nodes, relationships RETURN nodes, relationships", result -> {
            Map<String, Object> map = Iterators.single(result);
            List<Node> nodes = (List<Node>) map.get("nodes");
            List<Relationship> relationships = (List<Relationship>) map.get("relationships");
            assertEquals(6, nodes.size());
            assertEquals(2, nodes.stream().filter( node -> "Database".equals(Iterables.single(node.getLabels()).name())).count());
            assertEquals(2, nodes.stream().filter( node -> "DatabaseName".equals(Iterables.single(node.getLabels()).name())).count());
            assertEquals(1, nodes.stream().filter( node -> "User".equals(Iterables.single(node.getLabels()).name())).count());
            assertEquals(1, nodes.stream().filter( node -> "Version".equals(Iterables.single(node.getLabels()).name())).count());
            Set<String> names = nodes.stream().map(node -> (String)node.getProperty("name")).filter(Objects::nonNull).collect(Collectors.toSet());
            org.hamcrest.MatcherAssert.assertThat( names, Matchers.containsInAnyOrder("neo4j", "system"));

            assertEquals( 2, relationships.size() );
            assertEquals( 2, relationships.stream().filter( rel -> "TARGETS".equals( rel.getType().name() ) ).count() );
        });
    }

    @Test
    public void testExecute() {
        TestUtil.testResult(db, "CALL apoc.systemdb.execute('SHOW DATABASES YIELD name, default, currentStatus, home') YIELD row RETURN row", result -> {
            List<Map<String, Object>> rows = Iterators.asList(result.columnAs("row"));
            // removed key "systemDefault"
            org.hamcrest.MatcherAssert.assertThat(rows, Matchers.containsInAnyOrder(
                    MapUtil.map( "name", "system", "default", false, "currentStatus", "online", "home", false),
                    MapUtil.map("name", "neo4j", "default", true, "currentStatus", "online", "home", true)
            ));
        });
    }

    @Test
    public void testExecuteMultipleStatements() {
        // we have two databases, so asking twice returns 4
        assertEquals(4, TestUtil.count(db, "CALL apoc.systemdb.execute(['SHOW DATABASES','SHOW DATABASES'])"));
    }

    @Test
    public void testWriteStatements() {
        // count exhaust the result - this is important here
        TestUtil.count(db, "CALL apoc.systemdb.execute([\"CREATE USER dummy SET PASSWORD '12345678'\"])");

        assertEquals(2L, TestUtil.count(db, "CALL apoc.systemdb.execute('SHOW USERS')"));
    }
    
    @Test
    public void testExportMetadata() {
        // We test triggers
        final String triggerOne = "CALL apoc.trigger.add('firstTrigger', 'RETURN $alpha', {phase:\"after\"}, {params: {alpha:1}});";
        final String triggerTwo = "CALL apoc.trigger.add('beta', 'RETURN 1', {}, {params: {}});";
        // In this case we paused to test that it will be exported as paused
        final String pauseTrigger = "CALL apoc.trigger.pause('beta');";
        db.executeTransactionally(triggerOne);
        db.executeTransactionally(triggerTwo);
        db.executeTransactionally(pauseTrigger);

        // We test custom procedures and functions
        final String declareFunction = "CALL apoc.custom.declareFunction('declareFoo(input :: NUMBER?) :: (INTEGER?)', 'RETURN $input as answer', false, '');";
        db.executeTransactionally(declareFunction);
        final String declareProcedure = "CALL apoc.custom.declareProcedure('declareBar(one = 2 :: INTEGER?, two = 3 :: INTEGER?) :: (sum :: INTEGER?)', 'RETURN $one + $two as sum', 'READ', '');";
        db.executeTransactionally(declareProcedure);

        // We test uuid, we also need to export the related constraint (in another file)
        final String constraintForUuid = "CREATE CONSTRAINT Person_alpha IF NOT EXISTS FOR (n:Person) REQUIRE n.alpha IS UNIQUE;";
        db.executeTransactionally(constraintForUuid);
        final String uuidStatement = "CALL apoc.uuid.install('Person', {addToSetLabels:true, uuidProperty:\"alpha\"});";
        db.executeTransactionally(uuidStatement);

        // We test the data virtualization catalog
        final String dvStatement = "CALL apoc.dv.catalog.add('dvName', {desc:\"person's details\", labels:[\"Person\"], name:\"dvName\", params:[\"$name\", \"$age\"], query:\"map.name = $name and map.age = $age\", type:\"CSV\", url:\"file://myUrl\"})";
        db.executeTransactionally(dvStatement);

        TestUtil.testCall(db, "CALL apoc.systemdb.export.metadata", row -> {
            assertEquals(6L, row.get("rows"));
            assertEquals(true, row.get("done"));
            assertEquals("metadata", row.get("file"));
        });
        
        assertEquals(Set.of(constraintForUuid), readFileLines("metadata.Uuid.schema.neo4j.cypher", directory));
        assertEquals(Set.of(uuidStatement), readFileLines("metadata.Uuid.neo4j.cypher", directory));
        assertEquals(Set.of(triggerOne, triggerTwo, pauseTrigger), readFileLines("metadata.Trigger.neo4j.cypher", directory));
        assertEquals(Set.of(declareProcedure), readFileLines("metadata.CypherProcedure.neo4j.cypher", directory));
        assertEquals(Set.of(declareFunction), readFileLines("metadata.CypherFunction.neo4j.cypher", directory));
        assertEquals(Set.of(dvStatement), readFileLines("metadata.DataVirtualizationCatalog.neo4j.cypher", directory));
        
        // -- with config and uuid constrain dropped
        db.executeTransactionally("DROP CONSTRAINT Person_alpha");
        TestUtil.testCall(db, "CALL apoc.systemdb.export.metadata($config)", 
                Map.of("config", Map.of(FILENAME_KEY, "custom", FEATURES_KEY, Set.of(ExportMetadata.Type.Uuid.name()))), 
                row -> {
                    assertEquals(1L, row.get("rows"));
                    assertEquals(true, row.get("done"));
                    assertEquals("custom", row.get("file")); 
        });

        db.executeTransactionally("CALL apoc.uuid.removeAll");
        db.executeTransactionally("CALL apoc.trigger.removeAll");

        assertEquals(Set.of(constraintForUuid), readFileLines("custom.Uuid.schema.neo4j.cypher", directory));
        assertEquals(Set.of(uuidStatement), readFileLines("custom.Uuid.neo4j.cypher", directory));
    }

    private static Set<String> readFileLines(String fileName, File directory) {
        try {
            final List<String> fileLines = FileUtils.readLines(new File(directory, fileName), StandardCharsets.UTF_8);
            return new HashSet<>(fileLines);
        } catch ( IOException e) {
            throw new RuntimeException(e);
        }
    }
}
