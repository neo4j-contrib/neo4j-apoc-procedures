package apoc.export;

import apoc.export.csv.ExportCSV;
import apoc.export.csv.ImportCsv;
import apoc.export.cypher.ExportCypher;
import apoc.export.graphml.ExportGraphML;
import apoc.export.json.ExportJson;
import apoc.export.json.ImportJson;
import apoc.graph.Graphs;
import apoc.meta.Meta;
import apoc.periodic.PeriodicTestUtils;
import apoc.refactor.GraphRefactoring;
import apoc.refactor.rename.Rename;
import apoc.util.TestUtil;
import apoc.util.Util;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.Node;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static apoc.ApocConfig.APOC_EXPORT_FILE_ENABLED;
import static apoc.ApocConfig.APOC_IMPORT_FILE_ENABLED;
import static apoc.ApocConfig.apocConfig;
import static apoc.util.TransactionTestUtil.*;
import static java.util.Collections.emptyMap;
import static org.neo4j.configuration.GraphDatabaseSettings.TransactionStateMemoryAllocation.OFF_HEAP;
import static org.neo4j.configuration.SettingValueParsers.BYTES;

public class BigGraphTest {
    private static final File directory = new File("target/import");
    static { //noinspection ResultOfMethodCallIgnored
        directory.mkdirs();
    }
    
    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(GraphDatabaseSettings.memory_tracking, true)
            .withSetting(GraphDatabaseSettings.tx_state_memory_allocation, OFF_HEAP)
            .withSetting(GraphDatabaseSettings.tx_state_max_off_heap_memory, BYTES.parse("1G"))
            .withSetting(GraphDatabaseSettings.load_csv_file_url_root, directory.toPath().toAbsolutePath());

    @BeforeClass
    public static void setUp() throws Exception {
        TestUtil.registerProcedure(db, Rename.class, ExportCSV.class, ExportJson.class, ExportCypher.class, ExportGraphML.class, Graphs.class, Meta.class, ImportCsv.class, GraphRefactoring.class,
                ImportCsv.class, ImportJson.class);
        apocConfig().setProperty(APOC_IMPORT_FILE_ENABLED, true);
        apocConfig().setProperty(APOC_EXPORT_FILE_ENABLED, true);

        final String query = Util.readResourceFile("moviesMod.cypher");
        IntStream.range(0, 10000).forEach(__-> db.executeTransactionally(query));
    }

    @Test
    public void testTerminateExportCsv() {
        checkTerminationGuard(db, "CALL apoc.export.csv.all('testTerminate.csv',{bulkImport: true})");
    }

    @Test
    public void testTerminateExportGraphMl() {
        checkTerminationGuard(db, "CALL apoc.export.graphml.all('testTerminate.graphml', {})");
    }

    @Test
    public void testTerminateExportCypher() {
        checkTerminationGuard(db, "CALL apoc.export.cypher.all('testTerminate.cypher',{})");
    }

    @Test
    public void testTerminateExportJson() {
        checkTerminationGuard(db, "CALL apoc.export.json.all('testTerminate.json',{})");
    }

    @Test
    public void testTerminateRenameNodeProp() {
        // this procedure leverage the apoc.periodic.iterate, so we should check in the same way
        String innerLongQuery = "match (n) where n.`name` IS NOT NULL return n";
        String query = "CALL apoc.refactor.rename.nodeProperty('name', 'nameTwo')";

        PeriodicTestUtils.testTerminateWithCommand(db, query, innerLongQuery);
    }

    @Test
    public void testTerminateRenameType() {
        checkTerminationGuard(db, "CALL apoc.refactor.rename.type('DIRECTED', 'DIRECTED_TWO')");
    }

    @Test
    public void testTerminateRefactorCloneNodes() {
        List<Node> nodes = db.executeTransactionally("MATCH (n:Person) RETURN collect(n) as nodes", Collections.emptyMap(),
                r -> r.<List<Node>>columnAs("nodes").next());
        
        checkTerminationGuard(db, "CALL apoc.refactor.cloneNodes($nodes)",
                Map.of("nodes", nodes));
    }

    @Test
    public void testTerminateRefactorCloneSubgraph() {
        List<Node> nodes = db.executeTransactionally("MATCH (n:Person) RETURN collect(n) as nodes", Collections.emptyMap(),
                r -> r.<List<Node>>columnAs("nodes").next());

        checkTerminationGuard(db, "CALL apoc.refactor.cloneSubgraph($nodes)",
                Map.of("nodes", Util.rebind(nodes, db.beginTx())));
    }
}
