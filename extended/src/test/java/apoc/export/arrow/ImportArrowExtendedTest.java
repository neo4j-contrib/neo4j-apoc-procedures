package apoc.export.arrow;

import apoc.meta.Meta;
import apoc.util.TestUtil;
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
import java.util.Map;

import static apoc.ApocConfig.APOC_EXPORT_FILE_ENABLED;
import static apoc.ApocConfig.APOC_IMPORT_FILE_ENABLED;
import static apoc.ApocConfig.apocConfig;
import static apoc.export.arrow.ArrowTestUtil.ARROW_BASE_FOLDER;
import static apoc.export.arrow.ArrowTestUtil.testImportCommon;

public class ImportArrowExtendedTest {
    private static File directory = new File(ARROW_BASE_FOLDER);
    static { //noinspection ResultOfMethodCallIgnored
        directory.mkdirs();
    }

    private final Map<String, Object> MAPPING_ALL = Map.of("mapping",
            Map.of("bffSince", "Duration", "place", "Point", "listInt", "LongArray", "born", "LocalDateTime")
    );
    
    @ClassRule 
    public static DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(GraphDatabaseInternalSettings.enable_experimental_cypher_versions, true)
        .withSetting(GraphDatabaseSettings.load_csv_file_url_root, directory.toPath().toAbsolutePath());



    @BeforeClass
    public static void beforeClass() {
        TestUtil.registerProcedure(db, ExportArrowExtended.class, ImportArrow.class, Meta.class);
    }

    @Before
    public void before() {
        db.executeTransactionally("MATCH (n) DETACH DELETE n");

        db.executeTransactionally("CREATE (f:User {name:'Adam',age:42,male:true,kids:['Sam','Anna','Grace', 'Qwe'], born:localdatetime('2015-05-18T19:32:24.000'), place:point({latitude: 13.1, longitude: 33.46789, height: 100.0})})-[:KNOWS {since: 1993, bffSince: duration('P5M1.5D')}]->(b:User {name:'Jim',age:42})");
        db.executeTransactionally("CREATE (:Another {foo:1, listInt: [1,2]}), (:Another {bar:'Sam'})");

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
