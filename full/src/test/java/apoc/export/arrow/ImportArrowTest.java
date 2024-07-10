package apoc.export.arrow;

import apoc.meta.Meta;
import apoc.util.TestUtil;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.LocalDateTimeValue;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.virtual.VirtualValues;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import static apoc.ApocConfig.APOC_EXPORT_FILE_ENABLED;
import static apoc.ApocConfig.APOC_IMPORT_FILE_ENABLED;
import static apoc.ApocConfig.apocConfig;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class ImportArrowTest {
    private static File directory = new File("target/arrowImport");
    static { //noinspection ResultOfMethodCallIgnored
        directory.mkdirs();
    }

    private final Map<String, Object> MAPPING_ALL = Map.of("mapping",
            Map.of("bffSince", "Duration", "place", "Point", "listInt", "LongArray", "born", "LocalDateTime")
    );
    
    @ClassRule 
    public static DbmsRule db = new ImpermanentDbmsRule()
        .withSetting(GraphDatabaseSettings.load_csv_file_url_root, directory.toPath().toAbsolutePath());



    @BeforeClass
    public static void beforeClass() {
        TestUtil.registerProcedure(db, ExportArrow.class, ImportArrow.class, Meta.class);
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
        final byte[] bytes = db.executeTransactionally("CALL apoc.export.arrow.stream.all",
                Map.of(),
                this::extractByteArray);

        testImportCommon(bytes, MAPPING_ALL);
    }
    
    @Test
    public void testFileRoundtripImportArrowAll() {
        String file = db.executeTransactionally("CALL apoc.export.arrow.all('test_all.arrow') YIELD file",
                Map.of(),
                this::extractFileName);
        
        testImportCommon(file, MAPPING_ALL);
    }
    
    @Test
    public void testFileRoundtripImportArrowAllWithSmallBatchSize() {
        String file = db.executeTransactionally("CALL apoc.export.arrow.all('test_all.arrow') YIELD file",
                Map.of(),
                this::extractFileName);

        Map<String, Object> config = new HashMap<>(MAPPING_ALL);
        config.put("batchSize", 1);
        testImportCommon(file, config);
    }

    private void testImportCommon(Object file, Map<String, Object> config) {
        // then
        Map<String, Object> params = Map.of("file", file, "config", config);

        // remove current data
        db.executeTransactionally("MATCH (n) DETACH DELETE n");

        final String query = "CALL apoc.import.arrow($file, $config)";
        testCall(db, query, params,
                r -> {
                    assertEquals(4L, r.get("nodes"));
                    assertEquals(1L, r.get("relationships"));
                });

        testCall(db, "MATCH (start:User)-[rel:KNOWS]->(end:User) RETURN start, rel, end", r -> {
            Node start = (Node) r.get("start");
            assertFirstUserNodeProps(start.getAllProperties());
            Node end = (Node) r.get("end");
            assertSecondUserNodeProps(end.getAllProperties());
            Relationship rel = (Relationship) r.get("rel");
            assertRelationshipProps(rel.getAllProperties());
        });

        testResult(db, "MATCH (m:Another) RETURN m", r -> {
            ResourceIterator<Node> m = r.columnAs("m");
            Node node = m.next();
            assertFirstAnotherNodeProps(node.getAllProperties());
            node = m.next();
            assertSecondAnotherNodeProps(node.getAllProperties());
            assertFalse(r.hasNext());
        });
    }


    private String extractFileName(Result result) {
        return result.<String>columnAs("file").next();
    }
    
    private byte[] extractByteArray(Result result) {
        return result.<byte[]>columnAs("value").next();
    }

    private static void assertFirstUserNodeProps(Map<String, Object> props) {
        assertEquals("Adam", props.get("name"));
        assertEquals(42L, props.get("age"));
        assertEquals( true, props.get("male"));
        assertArrayEquals(new String[] { "Sam", "Anna", "Grace", "Qwe" }, (String[]) props.get("kids"));
        Map<String, Double> latitude = Map.of("latitude", 13.1D, "longitude", 33.46789D, "height", 100.0D);
        assertEquals(PointValue.fromMap(VirtualValues.map(latitude.keySet().toArray(new String[0]), latitude.values().stream().map(ValueUtils::of).toArray(AnyValue[]::new))),
                props.get("place"));
        assertEquals(LocalDateTimeValue.parse("2015-05-18T19:32:24.000").asObject(), props.get("born"));
    }

    private static void assertSecondUserNodeProps(Map<String, Object> props) {
        assertEquals( "Jim", props.get("name"));
        assertEquals(42L, props.get("age"));
    }

    private static void assertFirstAnotherNodeProps(Map<String, Object> map) {
        assertEquals(1L, map.get("foo"));
        assertArrayEquals(new long[] {1L, 2L}, (long[]) map.get("listInt"));
    }

    private static void assertSecondAnotherNodeProps(Map<String, Object> map) {
        assertEquals("Sam", map.get("bar"));
    }

    private static void assertRelationshipProps(Map<String, Object> props) {
        assertEquals(DurationValue.parse("P5M1DT12H"), props.get("bffSince"));
        assertEquals(1993L, props.get("since"));
    }
}
