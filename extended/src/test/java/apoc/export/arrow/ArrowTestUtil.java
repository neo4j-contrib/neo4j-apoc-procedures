package apoc.export.arrow;

import apoc.graph.Graphs;
import apoc.load.LoadArrow;
import apoc.meta.Meta;
import apoc.util.JsonUtil;
import apoc.util.TestUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.LocalDateTimeValue;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.virtual.VirtualValues;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static apoc.ApocConfig.APOC_EXPORT_FILE_ENABLED;
import static apoc.ApocConfig.APOC_IMPORT_FILE_ENABLED;
import static apoc.ApocConfig.apocConfig;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class ArrowTestUtil {
    public static String ARROW_BASE_FOLDER = "target/arrowImport";
    
    public static final List<Map<String, Object>> EXPECTED = List.of(
            new HashMap<>() {
                {
                    put("name", "Adam");
                    put("bffSince", null);
                    put("<source.id>", null);
                    put("<id>", 0L);
                    put("age", 42L);
                    put("labels", List.of("User"));
                    put("male", true);
                    put("<type>", null);
                    put("kids", List.of("Sam", "Anna", "Grace"));
                    put(
                            "place",
                            Map.of("crs", "wgs-84-3d", "longitude", 33.46789D, "latitude", 13.1D, "height", 100.0D));
                    put("<target.id>", null);
                    put("since", null);
                    put(
                            "born",
                            LocalDateTime.parse("2015-05-18T19:32:24.000")
                                    .atOffset(ZoneOffset.UTC)
                                    .toZonedDateTime());
                }
            },
            new HashMap<>() {
                {
                    put("name", "Jim");
                    put("bffSince", null);
                    put("<source.id>", null);
                    put("<id>", 1L);
                    put("age", 42L);
                    put("labels", List.of("User"));
                    put("male", null);
                    put("<type>", null);
                    put("kids", null);
                    put("place", null);
                    put("<target.id>", null);
                    put("since", null);
                    put("born", null);
                }
            },
            new HashMap<>() {
                {
                    put("name", null);
                    put("bffSince", "P5M1DT12H");
                    put("<source.id>", 0L);
                    put("<id>", 0L);
                    put("age", null);
                    put("labels", null);
                    put("male", null);
                    put("<type>", "KNOWS");
                    put("kids", null);
                    put("place", null);
                    put("<target.id>", 1L);
                    put("since", 1993L);
                    put("born", null);
                }
            });
    

    public static void beforeClassCommon(GraphDatabaseService db) {
        db.executeTransactionally(
                "CREATE (f:User {name:'Adam',age:42,male:true,kids:['Sam','Anna','Grace'], born:localdatetime('2015-05-18T19:32:24.000'), place:point({latitude: 13.1, longitude: 33.46789, height: 100.0})})-[:KNOWS {since: 1993, bffSince: duration('P5M1.5D')}]->(b:User {name:'Jim',age:42})");
        TestUtil.registerProcedure(db, ExportArrow.class, LoadArrow.class, ImportArrow.class, Graphs.class, Meta.class);

        apocConfig().setProperty(APOC_IMPORT_FILE_ENABLED, true);
        apocConfig().setProperty(APOC_EXPORT_FILE_ENABLED, true);
    }

    public static final Map<String, Object> MAPPING_ALL = Map.of("mapping",
            Map.of("bffSince", "Duration", "place", "Point", "listInt", "LongArray", "born", "LocalDateTime")
    );

    public static void createNodesForImportTests(DbmsRule db) {
        db.executeTransactionally("CREATE (:Another {foo:1, listInt: [1,2]}), (:Another {bar:'Sam'})");
    }

    public static void testLoadArrow(GraphDatabaseService db, String query, Map<String, Object> params) {
        db.executeTransactionally(query, params, result -> {
            final List<Map<String, Object>> actual = getActual(result);
            assertEquals(EXPECTED, actual);
            return null;
        });
    }

    private static List<Map<String, Object>> getActual(Result result) {
        return result.stream()
                .map(m -> (Map<String, Object>) m.get("value"))
                .map(m -> {
                    final Map<String, Object> newMap = new HashMap(m);
                    newMap.put("place", readValue((String) m.get("place"), Map.class));
                    return newMap;
                })
                .collect(Collectors.toList());
    }

    private static <T> T readValue(String json, Class<T> clazz) {
        if (json == null) return null;
        try {
            return JsonUtil.OBJECT_MAPPER.readValue(json, clazz);
        } catch (JsonProcessingException e) {
            return null;
        }
    }

    public static void testImportCommon(DbmsRule db, Object file, Map<String, Object> config) {
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

    public static String extractFileName(Result result) {
        return result.<String>columnAs("file").next();
    }

    public static byte[] extractByteArray(Result result) {
        return result.<byte[]>columnAs("value").next();
    }

    public static void assertFirstUserNodeProps(Map<String, Object> props) {
        assertEquals("Adam", props.get("name"));
        assertEquals(42L, props.get("age"));
        assertEquals( true, props.get("male"));
        assertArrayEquals(new String[] { "Sam", "Anna", "Grace" }, (String[]) props.get("kids"));
        Map<String, Double> latitude = Map.of("latitude", 13.1D, "longitude", 33.46789D, "height", 100.0D);
        assertEquals(PointValue.fromMap(VirtualValues.map(latitude.keySet().toArray(new String[0]), latitude.values().stream().map(ValueUtils::of).toArray(AnyValue[]::new))),
                props.get("place"));
        assertEquals(LocalDateTimeValue.parse("2015-05-18T19:32:24.000").asObject(), props.get("born"));
    }

    public static void assertSecondUserNodeProps(Map<String, Object> props) {
        assertEquals( "Jim", props.get("name"));
        assertEquals(42L, props.get("age"));
    }

    public static void assertFirstAnotherNodeProps(Map<String, Object> map) {
        assertEquals(1L, map.get("foo"));
        assertArrayEquals(new long[] {1L, 2L}, (long[]) map.get("listInt"));
    }

    public static void assertSecondAnotherNodeProps(Map<String, Object> map) {
        assertEquals("Sam", map.get("bar"));
    }

    public static void assertRelationshipProps(Map<String, Object> props) {
        assertEquals(DurationValue.parse("P5M1DT12H"), props.get("bffSince"));
        assertEquals(1993L, props.get("since"));
    }
}
