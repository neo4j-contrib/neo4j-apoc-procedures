package apoc.export.parquet;

import apoc.convert.ConvertUtils;
import apoc.graph.Graphs;
import apoc.load.LoadParquet;
import apoc.meta.Meta;
import apoc.util.TestUtil;
import apoc.util.collection.Iterators;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.LocalDateTimeValue;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.virtual.VirtualValues;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;

import static apoc.ApocConfig.APOC_EXPORT_FILE_ENABLED;
import static apoc.ApocConfig.APOC_IMPORT_FILE_ENABLED;
import static apoc.ApocConfig.apocConfig;
import static apoc.export.parquet.ParquetUtil.FIELD_ID;
import static apoc.export.parquet.ParquetUtil.FIELD_LABELS;
import static apoc.export.parquet.ParquetUtil.FIELD_SOURCE_ID;
import static apoc.export.parquet.ParquetUtil.FIELD_TARGET_ID;
import static apoc.export.parquet.ParquetUtil.FIELD_TYPE;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ParquetTestUtil {

    public static void beforeClassCommon(GraphDatabaseService db) {
        TestUtil.registerProcedure(db, ExportParquet.class, ImportParquet.class, LoadParquet.class, Graphs.class, Meta.class);
    }

    public static void beforeCommon(GraphDatabaseService db) {
        db.executeTransactionally("MATCH (n) DETACH DELETE n");

        db.executeTransactionally("CREATE (f:User {name:'Adam',age:42,male:true,kids:['Sam','Anna','Grace', 'Qwe'], born:localdatetime('2015-05-18T19:32:24.000'), place:point({latitude: 13.1, longitude: 33.46789, height: 100.0})})-[:KNOWS {since: 1993, bffSince: duration('P5M1.5D')}]->(b:User {name:'Jim',age:42})");
        db.executeTransactionally("CREATE (:Another {foo:1, listDate: [date('1999'), date('2000')], listInt: [1,2]}), (:Another {bar:'Sam'})");

        apocConfig().setProperty(APOC_IMPORT_FILE_ENABLED, true);
        apocConfig().setProperty(APOC_EXPORT_FILE_ENABLED, true);
    }

    public static void testImportAllCommon(GraphDatabaseService db, Map<String, Object> params) {
        // remove current data
        db.executeTransactionally("MATCH (n) DETACH DELETE n");

        final String query = "CALL apoc.import.parquet($file, $config)";
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

    public static void roundtripLoadAllAssertions(Result result) {
        ResourceIterator<Map<String, Object>> value = result.columnAs("value");
        Map<String, Object> actual = value.next();
        assertFirstUserNode(actual);
        actual = value.next();
        assertSecondUserNode(actual);
        actual = value.next();
        assertFirstAnotherNode(actual);
        actual = value.next();
        assertSecondAnotherNode(actual);
        actual = value.next();
        assertRelationship(actual);
        assertFalse(value.hasNext());
    }

    public static void assertNodeAndLabel(Map<String, Object> startTwo, String label) {
        assertTrue(startTwo.get(FIELD_ID) instanceof Long);
        assertEquals(ValueUtils.of(List.of(label)), ValueUtils.of(startTwo.get(FIELD_LABELS)));
    }

    public static void assertBarRel(String one, Map<String, Object> relTwo) {
        assertEquals(one, relTwo.get("idRel"));
        assertEquals("BAR", relTwo.get(FIELD_TYPE));
        assertTrue(relTwo.get(FIELD_ID) instanceof Long);
        assertTrue(relTwo.get(FIELD_SOURCE_ID) instanceof Long);
        assertTrue(relTwo.get(FIELD_TARGET_ID) instanceof Long);
    }

    public static String extractFileName(Result result) {
        return Iterators.single(result.columnAs("file"));
    }

    private static void assertFirstUserNode(Map<String, Object> map) {
        assertNodeAndLabel(map, "User");
        assertFirstUserNodeProps(map);
    }

    private static void assertSecondUserNode(Map<String, Object> map) {
        assertNodeAndLabel(map, "User");
        assertSecondUserNodeProps(map);
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

    private static void assertFirstAnotherNode(Map<String, Object> map) {
        assertNodeAndLabel(map, "Another");
        assertFirstAnotherNodeProps(map);
    }

    private static void assertFirstAnotherNodeProps(Map<String, Object> map) {
        assertEquals(1L, map.get("foo"));
        List<LocalDate> listDate = ConvertUtils.convertToList(map.get("listDate"));
        assertEquals(2, listDate.size());
        assertEquals(LocalDate.of(1999, 1, 1), listDate.get(0));
        assertEquals(LocalDate.of(2000, 1, 1), listDate.get(1));
        assertArrayEquals(new long[] {1L, 2L}, (long[]) map.get("listInt"));
    }

    private static void assertSecondAnotherNode(Map<String, Object> map) {
        assertNodeAndLabel(map, "Another");
        assertSecondAnotherNodeProps(map);
    }

    private static void assertSecondAnotherNodeProps(Map<String, Object> map) {
        assertEquals("Sam", map.get("bar"));
    }

    private static void assertRelationship(Map<String, Object> map) {
        assertTrue(map.get(FIELD_SOURCE_ID) instanceof Long);
        assertTrue(map.get(FIELD_TARGET_ID) instanceof Long);
        assertRelationshipProps(map);
    }

    private static void assertRelationshipProps(Map<String, Object> props) {
        assertEquals(DurationValue.parse("P5M1DT12H"), props.get("bffSince"));
        assertEquals(1993L, props.get("since"));
    }

}
