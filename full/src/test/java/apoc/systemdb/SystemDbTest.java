package apoc.systemdb;

import apoc.ApocConfig;
import apoc.custom.CypherProcedures;
import apoc.cypher.CypherExtended;
import apoc.dv.DataVirtualizationCatalog;
import apoc.periodic.Periodic;
import apoc.systemdb.metadata.ExportMetadata;
import apoc.trigger.Trigger;
import apoc.util.TestUtil;
import apoc.uuid.Uuid;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.helpers.collection.MapUtil;
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
import static apoc.util.TestUtil.readFileLines;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
        apocConfig().setProperty(ApocConfig.APOC_UUID_ENABLED, true);
        apocConfig().setProperty(ApocConfig.APOC_TRIGGER_ENABLED, true);
        TestUtil.registerProcedure(db, SystemDb.class, Trigger.class, CypherProcedures.class, Uuid.class, Periodic.class, DataVirtualizationCatalog.class, CypherExtended.class);
    }

    @Test
    public void testGetGraph() throws Exception {
        TestUtil.testResult(db, "CALL apoc.systemdb.graph() YIELD nodes, relationships RETURN nodes, relationships", result -> {
            Map<String, Object> map = Iterators.single(result);
            List<Node> nodes = (List<Node>) map.get("nodes");
            List<Relationship> relationships = (List<Relationship>) map.get("relationships");

            assertEquals(4, nodes.size());
            assertEquals(2, nodes.stream().filter(node -> "Database".equals(Iterables.single(node.getLabels()).name())).count());
            assertEquals(1, nodes.stream().filter(node -> "User".equals(Iterables.single(node.getLabels()).name())).count());
            assertEquals(1, nodes.stream().filter(node -> "Version".equals(Iterables.single(node.getLabels()).name())).count());
            Set<String> names = nodes.stream().map(node -> (String) node.getProperty("name")).filter(Objects::nonNull).collect(Collectors.toSet());
            org.hamcrest.MatcherAssert.assertThat(names, Matchers.containsInAnyOrder("neo4j", "system"));

            assertTrue(relationships.isEmpty());
        });
    }

    @Test
    public void testExecute() {
        TestUtil.testResult(db, "CALL apoc.systemdb.execute('SHOW DATABASES') YIELD row RETURN row", result -> {
            List<Map<String, Object>> rows = Iterators.asList(result.columnAs("row"));
            org.hamcrest.MatcherAssert.assertThat(rows, Matchers.containsInAnyOrder(
                    MapUtil.map("name", "system", "default", false, "currentStatus", "online", "role", "standalone", "requestedStatus", "online", "error", "", "address", "localhost:7687"),
                    MapUtil.map("name", "neo4j", "default", true, "currentStatus", "online", "role", "standalone", "requestedStatus", "online", "error", "", "address", "localhost:7687")
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
        TestUtil.count(db, "CALL apoc.systemdb.execute([\"CREATE USER dummy SET PASSWORD '123'\"])");

        assertEquals(2l, TestUtil.count(db, "CALL apoc.systemdb.execute('SHOW USERS')"));
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

        // We test custom procedures and functions with deprecated syntax
        // the expected exported cypher queries will leverage the new procedures (declareFunction and declareProcedure) 
        db.executeTransactionally("CALL apoc.custom.asProcedure('procName','RETURN $input as answer','read',[['answer','number']],[['input','int','42']], 'Procedure that answer to the Ultimate Question of Life, the Universe, and Everything');");
        db.executeTransactionally("CALL apoc.custom.asFunction('funName','RETURN $input as answer', 'long', [['input','number']], false);");
        String declareStatementFromFunction = "CALL apoc.custom.declareFunction('funName(input :: NUMBER?) :: (INTEGER?)', 'RETURN $input as answer', false, '');";
        String declareStatementFromProcedure = "CALL apoc.custom.declareProcedure('procName(input = 42 :: INTEGER?) :: (answer :: NUMBER?)', 'RETURN $input as answer', 'READ', 'Procedure that answer to the Ultimate Question of Life, the Universe, and Everything');";

        // We test uuid, we also need to export the related constraint (in another file)
        final String constraintForUuid = "CREATE CONSTRAINT ON (n:Person) ASSERT n.alpha IS UNIQUE;";
        db.executeTransactionally(constraintForUuid);
        final String uuidStatement = "CALL apoc.uuid.install('Person', {addToSetLabels:true, uuidProperty:\"alpha\"});";
        db.executeTransactionally(uuidStatement);

        // We test the data virtualization catalog
        final String dvStatement = "CALL apoc.dv.catalog.add('dvName', {desc:\"person's details\", labels:[\"Person\"], name:\"dvName\", params:[\"$name\", \"$age\"], query:\"map.name = $name and map.age = $age\", type:\"CSV\", url:\"file://myUrl\"})";
        db.executeTransactionally(dvStatement);

        TestUtil.testCall(db, "CALL apoc.systemdb.export.metadata", row -> {
            assertEquals(8L, row.get("rows"));
            assertEquals(true, row.get("done"));
            assertEquals("metadata", row.get("file"));
        });
        
        assertEquals(Set.of(constraintForUuid), readFileLines("metadata.Uuid.schema.neo4j.cypher", directory));
        assertEquals(Set.of(uuidStatement), readFileLines("metadata.Uuid.neo4j.cypher", directory));
        assertEquals(Set.of(triggerOne, triggerTwo, pauseTrigger), readFileLines("metadata.Trigger.neo4j.cypher", directory));
        assertEquals(Set.of(declareProcedure, declareStatementFromProcedure), readFileLines("metadata.CypherProcedure.neo4j.cypher", directory));
        assertEquals(Set.of(declareFunction, declareStatementFromFunction), readFileLines("metadata.CypherFunction.neo4j.cypher", directory));
        assertEquals(Set.of(dvStatement), readFileLines("metadata.DataVirtualizationCatalog.neo4j.cypher", directory));
        
        // -- with config and uuid constrain dropped
        db.executeTransactionally("DROP CONSTRAINT ON (p:Person) ASSERT p.alpha IS UNIQUE");
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
}
