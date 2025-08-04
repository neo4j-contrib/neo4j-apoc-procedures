package apoc.export.arrow;

import apoc.meta.Meta;
import apoc.meta.MetaRestricted;
import apoc.util.TestUtil;
import apoc.util.Util;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static apoc.ApocConfig.APOC_EXPORT_FILE_ENABLED;
import static apoc.ApocConfig.APOC_IMPORT_FILE_ENABLED;
import static apoc.ApocConfig.apocConfig;
import static apoc.export.arrow.ArrowTestUtil.ARROW_BASE_FOLDER;
import static apoc.export.arrow.ArrowTestUtil.testImportCommon;
import static org.neo4j.configuration.SettingImpl.newBuilder;
import static org.neo4j.configuration.SettingValueParsers.BOOL;

public class ImportArrowExtendedTest {
    private static File directory = new File(ARROW_BASE_FOLDER);
    static { //noinspection ResultOfMethodCallIgnored
        directory.mkdirs();
    }

    private final Map<String, Object> MAPPING_ALL = Map.of("mapping",
            Util.map("bffSince", "Duration", 
                    "place", "Point", 
                    "listInt", "LongArray", 
                    "born", "LocalDateTime",
                    "INTEGER64", "vector",
                    "INTEGER32", "vector",
                    "INTEGER16", "vector",
                    "INTEGER8", "vector",
                    "FLOAT64", "vector",
                    "FLOAT32", "vector"
            )
    );
    
    @ClassRule 
    public static DbmsRule db = new ImpermanentDbmsRule()

            .withSetting(
                    GraphDatabaseSettings.procedure_unrestricted,
                    List.of(
                            "apoc.meta.nodes.count",
                            "apoc.meta.stats",
                            "apoc.meta.data",
                            "apoc.meta.schema",
                            "apoc.meta.nodeTypeProperties",
                            "apoc.meta.relTypeProperties",
                            "apoc.meta.graph",
                            "apoc.meta.graph.of",
                            "apoc.meta.graphSample",
                            "apoc.meta.subGraph"))
            .withSetting(GraphDatabaseInternalSettings.cypher_enable_vector_type, true)
            .withSetting(
                    newBuilder("internal.dbms.debug.track_cursor_close", BOOL, false)
                            .build(),
                    false)
            .withSetting(
                    newBuilder("internal.dbms.debug.trace_cursors", BOOL, false).build(), false)
            .withSetting(GraphDatabaseInternalSettings.cypher_enable_vector_type, true)
            .withSetting(GraphDatabaseInternalSettings.enable_experimental_cypher_versions, true)
            .withSetting(GraphDatabaseSettings.load_csv_file_url_root, directory.toPath().toAbsolutePath());



    @BeforeClass
    public static void beforeClass() {
        TestUtil.registerProcedure(db, ExportArrowExtended.class, ImportArrow.class, Meta.class, MetaRestricted.class);
    }

    @Before
    public void before() {
        db.executeTransactionally("MATCH (n) DETACH DELETE n");

        var vectorTypes1 = List.of("INT64", "INT32", "INT16", "INT8", "FLOAT64", "FLOAT32");
        for (String type : vectorTypes1) {
            db.executeTransactionally("CYPHER 25 CREATE (:Foo { z: VECTOR([1, 2, 3], 3, %s) });".formatted(type));
        }

        db.executeTransactionally("CREATE (f:User {name:'Adam',age:42,male:true,kids:['Sam','Anna','Grace'], born:localdatetime('2015-05-18T19:32:24.000'), place:point({latitude: 13.1, longitude: 33.46789, height: 100.0})})-[:KNOWS {since: 1993, bffSince: duration('P5M1.5D')}]->(b:User {name:'Jim',age:42})");
        db.executeTransactionally("CREATE (:Another {foo:1, listInt: [1,2]}), (:Another {bar:'Sam'})");
//        var vectorTypes = List.of("INTEGER64", "INTEGER32", "INTEGER16", "INTEGER8", "FLOAT64","FLOAT32");
        var vectorTypes = List.of("INT64", "INT32", "INT16", "INT8", "FLOAT64", "FLOAT32");
        var types = vectorTypes.stream().map(i -> "%1$s: VECTOR([1,2,3], 3, %1$s)".formatted(i)).collect(Collectors.joining(","));
        db.executeTransactionally("CYPHER 25 CREATE (:Vectors {%s})".formatted(types));

        apocConfig().setProperty(APOC_IMPORT_FILE_ENABLED, true);
        apocConfig().setProperty(APOC_EXPORT_FILE_ENABLED, true);
    }

    @Test
    public void testStreamRoundtripImportArrowAll() {
        final byte[] bytes = db.executeTransactionally("CYPHER 25 CALL apoc.export.arrow.stream.all",
                Map.of(),
                ArrowTestUtil::extractByteArray);

        testImportCommon(db, bytes, MAPPING_ALL);
    }
    
    @Test
    public void testFileRoundtripImportArrowAll() {
        String file = db.executeTransactionally("CYPHER 25 CALL apoc.export.arrow.all('test_all.arrow') YIELD file",
                Map.of(),
                ArrowTestUtil::extractFileName);
        
        testImportCommon(db, file, MAPPING_ALL);
    }
    
    @Test
    public void testFileRoundtripImportArrowAllWithSmallBatchSize() {
        String file = db.executeTransactionally("CYPHER 25 CALL apoc.export.arrow.all('test_all.arrow') YIELD file",
                Map.of(),
                ArrowTestUtil::extractFileName);

        Map<String, Object> config = new HashMap<>(MAPPING_ALL);
        config.put("batchSize", 1);
        testImportCommon(db, file, config);
    }
}
