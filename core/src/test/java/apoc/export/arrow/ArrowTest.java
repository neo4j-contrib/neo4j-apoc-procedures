/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package apoc.export.arrow;

import apoc.ApocSettings;
import apoc.graph.Graphs;
import apoc.load.LoadArrow;
import apoc.meta.Meta;
import apoc.util.JsonUtil;
import apoc.util.TestUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.Result;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.io.File;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static apoc.ApocConfig.APOC_EXPORT_FILE_ENABLED;
import static apoc.ApocConfig.APOC_IMPORT_FILE_ENABLED;
import static apoc.ApocConfig.apocConfig;
import static org.junit.Assert.assertEquals;

public class ArrowTest {

    private static File directory = new File("target/arrow import");
    static { //noinspection ResultOfMethodCallIgnored
        directory.mkdirs();
    }

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(ApocSettings.apoc_import_file_enabled, true)
            .withSetting(GraphDatabaseSettings.load_csv_file_url_root, directory.toPath().toAbsolutePath())
            .withSetting(ApocSettings.apoc_export_file_enabled, true);

    public static final List<Map<String, Object>> EXPECTED = List.of(
            new HashMap<>() {{
                put("name", "Adam");
                put("bffSince", null);
                put("<source.id>", null);
                put("<id>", 0L);
                put("age", 42L);
                put("labels", List.of("User"));
                put("male", true);
                put("<type>", null);
                put("kids", List.of("Sam", "Anna", "Grace"));
                put("place", Map.of("crs", "wgs-84-3d",
                        "longitude", 33.46789D,
                        "latitude", 13.1D,
                        "height", 100.0D));
                put("<target.id>", null);
                put("since", null);
                put("born", LocalDateTime.parse("2015-05-18T19:32:24.000").atOffset(ZoneOffset.UTC).toZonedDateTime());
            }},
            new HashMap<>() {{
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
            }},
            new HashMap<>() {{
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
            }}
    );

    @BeforeClass
    public static void beforeClass() {
        db.executeTransactionally("CREATE (f:User {name:'Adam',age:42,male:true,kids:['Sam','Anna','Grace'], born:localdatetime('2015-05-18T19:32:24.000'), place:point({latitude: 13.1, longitude: 33.46789, height: 100.0})})-[:KNOWS {since: 1993, bffSince: duration('P5M1.5D')}]->(b:User {name:'Jim',age:42})");
        TestUtil.registerProcedure(db, ExportArrow.class, LoadArrow.class, Graphs.class, Meta.class);
    }

    @Before
    public void before() {
        apocConfig().setProperty(APOC_IMPORT_FILE_ENABLED, true);
        apocConfig().setProperty(APOC_EXPORT_FILE_ENABLED, true);
    }

    private byte[] extractByteArray(Result result) {
        return result.<byte[]>columnAs("byteArray").next();
    }

    private String extractFileName(Result result) {
        return result.<String>columnAs("file").next();
    }

    private <T> T readValue(String json, Class<T> clazz) {
        if (json == null) return null;
        try {
            return JsonUtil.OBJECT_MAPPER.readValue(json, clazz);
        } catch (JsonProcessingException e) {
            return null;
        }
    }

    @Test
    public void testStreamRoundtripArrowQuery() {
        // given - when
        final String returnQuery = "RETURN 1 AS intData," +
                "'a' AS stringData," +
                "true AS boolData," +
                "[1, 2, 3] AS intArray," +
                "[1.1, 2.2, 3.3] AS doubleArray," +
                "[true, false, true] AS boolArray," +
                "[1, '2', true, null] AS mixedArray," +
                "{foo: 'bar'} AS mapData," +
                "localdatetime('2015-05-18T19:32:24') as dateData," +
                "[[0]] AS arrayArray," +
                "1.1 AS doubleData";
        final byte[] byteArray = db.executeTransactionally("CALL apoc.export.arrow.stream.query($query) YIELD value AS byteArray",
                Map.of("query", returnQuery),
                this::extractByteArray);

        // then
        final String query = "CALL apoc.load.arrow.stream($byteArray) YIELD value " +
                "RETURN value";
        db.executeTransactionally(query, Map.of("byteArray", byteArray), result -> {
            final Map<String, Object> row = (Map<String, Object>) result.next().get("value");
            assertEquals(1L, row.get("intData"));
            assertEquals("a", row.get("stringData"));
            assertEquals(Arrays.asList(1L, 2L, 3L), row.get("intArray"));
            assertEquals(Arrays.asList(1.1D, 2.2D, 3.3), row.get("doubleArray"));
            assertEquals(Arrays.asList(true, false, true), row.get("boolArray"));
            assertEquals(Arrays.asList("1", "2", "true", null), row.get("mixedArray"));
            assertEquals("{\"foo\":\"bar\"}", row.get("mapData"));
            assertEquals(LocalDateTime.parse("2015-05-18T19:32:24.000")
                    .atOffset(ZoneOffset.UTC)
                    .toZonedDateTime(), row.get("dateData"));
            assertEquals(Arrays.asList("[0]"), row.get("arrayArray"));
            assertEquals(1.1D, row.get("doubleData"));
            return true;
        });
    }

    @Test
    public void testFileRoundtripArrowQuery() {
        // given - when
        final String returnQuery = "RETURN 1 AS intData," +
                "'a' AS stringData," +
                "true AS boolData," +
                "[1, 2, 3] AS intArray," +
                "[1.1, 2.2, 3.3] AS doubleArray," +
                "[true, false, true] AS boolArray," +
                "[1, '2', true, null] AS mixedArray," +
                "{foo: 'bar'} AS mapData," +
                "localdatetime('2015-05-18T19:32:24') as dateData," +
                "[[0]] AS arrayArray," +
                "1.1 AS doubleData";
        String file = db.executeTransactionally("CALL apoc.export.arrow.query('query_test.arrow', $query) YIELD file",
                Map.of("query", returnQuery),
                this::extractFileName);

        // then
        final String query = "CALL apoc.load.arrow($file) YIELD value " +
                "RETURN value";
        db.executeTransactionally(query,
                Map.of("file", file),
                result -> {
                    final Map<String, Object> row = (Map<String, Object>) result.next().get("value");
                    assertEquals(1L, row.get("intData"));
                    assertEquals("a", row.get("stringData"));
                    assertEquals(Arrays.asList(1L, 2L, 3L), row.get("intArray"));
                    assertEquals(Arrays.asList(1.1D, 2.2D, 3.3), row.get("doubleArray"));
                    assertEquals(Arrays.asList(true, false, true), row.get("boolArray"));
                    assertEquals(Arrays.asList("1", "2", "true", null), row.get("mixedArray"));
                    assertEquals("{\"foo\":\"bar\"}", row.get("mapData"));
                    assertEquals(LocalDateTime.parse("2015-05-18T19:32:24.000")
                            .atOffset(ZoneOffset.UTC)
                            .toZonedDateTime(), row.get("dateData"));
                    assertEquals(Arrays.asList("[0]"), row.get("arrayArray"));
                    assertEquals(1.1D, row.get("doubleData"));
                    return true;
                });
    }

    @Test
    public void testStreamRoundtripArrowGraph() {
        // given - when
        final byte[] byteArray = db.executeTransactionally("CALL apoc.graph.fromDB('neo4j',{}) yield graph " +
                        "CALL apoc.export.arrow.stream.graph(graph) YIELD value AS byteArray " +
                        "RETURN byteArray",
                Map.of(),
                this::extractByteArray);

        // then
        final String query = "CALL apoc.load.arrow.stream($byteArray) YIELD value " +
                "RETURN value";
        db.executeTransactionally(query, Map.of("byteArray", byteArray), result -> {
            final List<Map<String, Object>> actual = getActual(result);
            assertEquals(EXPECTED, actual);
            return null;
        });
    }

    private List<Map<String, Object>> getActual(Result result) {
        return result.stream()
                .map(m -> (Map<String, Object>) m.get("value"))
                .map(m -> {
                    final Map<String, Object> newMap = new HashMap(m);
                    newMap.put("place", readValue((String) m.get("place"), Map.class));
                    return newMap;
                })
                .collect(Collectors.toList());
    }

    @Test
    public void testFileRoundtripArrowGraph() {
        // given - when
        String file = db.executeTransactionally("CALL apoc.graph.fromDB('neo4j',{}) yield graph " +
                        "CALL apoc.export.arrow.graph('graph_test.arrow', graph) YIELD file " +
                        "RETURN file",
                Map.of(),
                this::extractFileName);

        // then
        final String query = "CALL apoc.load.arrow($file) YIELD value " +
                "RETURN value";
        db.executeTransactionally(query, Map.of("file", file), result -> {
            final List<Map<String, Object>> actual = getActual(result);
            assertEquals(EXPECTED, actual);
            return null;
        });
    }

    @Test
    public void testStreamRoundtripArrowAll() {
        testStreamRoundtripAllCommon();
    }

    @Test
    public void testStreamRoundtripArrowAllWithImportExportConfsDisabled() {
        // disable both export and import configs
        apocConfig().setProperty(APOC_IMPORT_FILE_ENABLED, false);
        apocConfig().setProperty(APOC_EXPORT_FILE_ENABLED, false);

        // should work regardless of the previous config
        testStreamRoundtripAllCommon();
    }

    private void testStreamRoundtripAllCommon() {
        // given - when
        final byte[] byteArray = db.executeTransactionally("CALL apoc.export.arrow.stream.all() YIELD value AS byteArray ",
                Map.of(),
                this::extractByteArray);

        // then
        final String query = "CALL apoc.load.arrow.stream($byteArray) YIELD value " +
                "RETURN value";
        db.executeTransactionally(query, Map.of("byteArray", byteArray), result -> {
            final List<Map<String, Object>> actual = getActual(result);
            assertEquals(EXPECTED, actual);
            return null;
        });
    }

    @Test
    public void testFileRoundtripArrowAll() {
        // given - when
        String file = db.executeTransactionally("CALL apoc.export.arrow.all('all_test.arrow') YIELD file",
                Map.of(),
                this::extractFileName);

        // then
        final String query = "CALL apoc.load.arrow($file) YIELD value " +
                "RETURN value";
        db.executeTransactionally(query, Map.of("file", file), result -> {
            final List<Map<String, Object>> actual = getActual(result);
            assertEquals(EXPECTED, actual);
            return null;
        });
    }

    @Test
    public void testStreamVolumeArrowAll() {
        // given - when
        db.executeTransactionally("UNWIND range(0, 10000 - 1) AS id CREATE (n:ArrowNode{id:id})");

        final List<byte[]> list = db.executeTransactionally("CALL apoc.export.arrow.stream.query('MATCH (n:ArrowNode) RETURN n.id AS id') YIELD value AS byteArray ",
                Map.of(),
                result -> result.<byte[]>columnAs("byteArray").stream().collect(Collectors.toList()));

        final List<Long> expected = LongStream.range(0, 10000)
                .mapToObj(l -> l)
                .collect(Collectors.toList());

        // then
        final String query = "UNWIND $list AS byteArray " +
                "CALL apoc.load.arrow.stream(byteArray) YIELD value " +
                "RETURN value.id AS id";
        db.executeTransactionally(query, Map.of("list", list), result -> {
            final List<Long> actual = result.stream()
                    .map(m -> (Long) m.get("id"))
                    .sorted()
                    .collect(Collectors.toList());
            assertEquals(expected, actual);
            return null;
        });

        db.executeTransactionally("MATCH (n:ArrowNode) DELETE n");
    }

    @Test
    public void testFileVolumeArrowAll() {
        // given - when
        db.executeTransactionally("UNWIND range(0, 10000 - 1) AS id CREATE (:ArrowNode{id:id})");

        String file = db.executeTransactionally("CALL apoc.export.arrow.query('volume_test.arrow', 'MATCH (n:ArrowNode) RETURN n.id AS id') YIELD file ",
                Map.of(),
                this::extractFileName);

        final List<Long> expected = LongStream.range(0, 10000)
                .mapToObj(l -> l)
                .collect(Collectors.toList());

        // then
        final String query = "CALL apoc.load.arrow($file) YIELD value " +
                "RETURN value.id AS id";
        db.executeTransactionally(query, Map.of("file", file), result -> {
            final List<Long> actual = result.stream()
                    .map(m -> (Long) m.get("id"))
                    .sorted()
                    .collect(Collectors.toList());
            assertEquals(expected, actual);
            return null;
        });

        db.executeTransactionally("MATCH (n:ArrowNode) DELETE n");
    }

    @Test
    public void testValidNonStorableQuery() {
        final List<byte[]> list = db.executeTransactionally("CALL apoc.export.arrow.stream.query($query) YIELD value AS byteArray ",
                Map.of("query", "RETURN [1, true, 2.3, null, { name: 'Dave' }] AS array"),
                result -> result.<byte[]>columnAs("byteArray").stream().collect(Collectors.toList()));

        final List<String> expected = Arrays.asList("1", "true", "2.3", null, "{\"name\":\"Dave\"}");

        // then
        final String query = "UNWIND $list AS byteArray " +
                "CALL apoc.load.arrow.stream(byteArray) YIELD value " +
                "RETURN value.array AS array";
        db.executeTransactionally(query, Map.of("list", list), result -> {
            List<String> actual = result.<List<String>>columnAs("array").next();
            assertEquals(expected, actual);
            return null;
        });

    }


}