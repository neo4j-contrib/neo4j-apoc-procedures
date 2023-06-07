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
package apoc.export.cypher;

import apoc.export.util.ExportConfig;
import apoc.util.BinaryTestUtil;
import apoc.util.CompressionAlgo;
import apoc.util.MapUtil;
import apoc.util.TestUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static apoc.export.cypher.ExportCypherTest.ExportCypherResults.*;
import static apoc.export.util.ExportFormat.*;
import static apoc.util.BinaryTestUtil.getDecompressedData;
import static apoc.util.TestUtil.assertError;
import static apoc.util.Util.INVALID_QUERY_MODE_ERROR;
import static apoc.util.Util.map;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.*;
import static org.neo4j.configuration.SettingImpl.newBuilder;
import static org.neo4j.configuration.SettingValueParsers.BOOL;

/**
 * @author mh
 * @since 22.05.16
 */
public class ExportCypherTest {
    private static final String queryWithRelOnly = "MATCH (start:Foo)-[rel:KNOWS]->(end:Bar) RETURN rel";
    private static final String queryWithStartAndRel = "MATCH (start:Foo)-[rel:KNOWS]->(end:Bar) RETURN start, rel";
    private final static String exportQuery = "CALL apoc.export.cypher.query($query, $file, $config)";

    private static final String exportDataWithStartAndRel = "MATCH (start:Foo)-[rel:KNOWS]->(end:Bar) " +
            "CALL apoc.export.cypher.data([start], [rel], $file, $config) " +
            "YIELD nodes, relationships, properties RETURN *";
    private static final String exportDataWithRelOnly = "MATCH (start:Foo)-[rel:KNOWS]->(end:Bar) " +
            "CALL apoc.export.cypher.data([], [rel], $file, $config) " +
            "YIELD nodes, relationships, properties RETURN *";

    private static final Map<String, Object> exportConfig = map("useOptimizations", map("type", "none"), "separateFiles", true, "format", "neo4j-admin");

    private static File directory = new File("target/import");

    static { //noinspection ResultOfMethodCallIgnored
        directory.mkdirs();
    }

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(GraphDatabaseSettings.load_csv_file_url_root, directory.toPath().toAbsolutePath())
            .withSetting(newBuilder( "unsupported.dbms.debug.track_cursor_close", BOOL, false ).build(), false)
            .withSetting(newBuilder( "unsupported.dbms.debug.trace_cursors", BOOL, false ).build(), false);

    @Rule
    public TestName testName = new TestName();

    @Before
    public void setUp() throws Exception {
        ExportCypherTestUtils.setUp(db, testName);
    }

    @Test
    public void testExportAllCypherResults() {
        TestUtil.testCall(db, "CALL apoc.export.cypher.all(null,{useOptimizations: { type: 'none'}, format: 'neo4j-shell'})", (r) -> {
            assertResults(null, r, "database");
            assertEquals(EXPECTED_NEO4J_SHELL, r.get("cypherStatements"));
        });
    }

    @Test
    public void testExportAllCypherStreaming() {
        final String query = "CALL apoc.export.cypher.all(null,{useOptimizations: { type: 'none'}, streamStatements:true,batchSize:3, format: 'neo4j-shell'})";
        assertExportAllCypherStreaming(CompressionAlgo.NONE, query);
    }

    @Test
    public void testExportAllWithCompressionCypherStreaming() {
        final CompressionAlgo algo = CompressionAlgo.BZIP2;
        final String query = "CALL apoc.export.cypher.all(null,{compression: '" + algo.name() + "', useOptimizations: { type: 'none'}, streamStatements:true,batchSize:3, format: 'neo4j-shell'})";
        assertExportAllCypherStreaming(algo, query);
    }
    
    @Test
    public void testExportAllWithStreamingSeparatedAndCompressedFile() {
        final CompressionAlgo algo = CompressionAlgo.BZIP2;
        final Map<String, Object> config = map("compression", algo.name(),
                "useOptimizations", map("type", "none"), 
                "stream", true, "separateFiles", true, "format", "neo4j-admin");
        
        TestUtil.testCall(db, "CALL apoc.export.cypher.all(null, $config)", map("config", config), r -> {
            assertEquals(EXPECTED_NODES, getDecompressedData(algo, r.get("nodeStatements")));
            assertEquals(EXPECTED_RELATIONSHIPS, getDecompressedData(algo, r.get("relationshipStatements")));
            assertEquals(EXPECTED_SCHEMA, getDecompressedData(algo, r.get("schemaStatements")));
            assertEquals(EXPECTED_CLEAN_UP, getDecompressedData(algo, r.get("cleanupStatements")));
        });
    }

    private void assertExportAllCypherStreaming(CompressionAlgo algo, String query) {
        StringBuilder sb = new StringBuilder();
        TestUtil.testResult(db, query, (res) -> {
            Map<String, Object> r = res.next();
            assertEquals(3L, r.get("batchSize"));
            assertEquals(1L, r.get("batches"));
            assertEquals(3L, r.get("nodes"));
            assertEquals(3L, r.get("rows"));
            assertEquals(0L, r.get("relationships"));
            assertEquals(5L, r.get("properties"));
            assertNull("Should get file",r.get("file"));
            assertEquals("cypher", r.get("format"));
            assertTrue("Should get time greater than 0",((long) r.get("time")) >= 0);
            sb.append(getDecompressedData(algo, r.get("cypherStatements")));
            r = res.next();
            assertEquals(3L, r.get("batchSize"));
            assertEquals(2L, r.get("batches"));
            assertEquals(3L, r.get("nodes"));
            assertEquals(4L, r.get("rows"));
            assertEquals(1L, r.get("relationships"));
            assertEquals(6L, r.get("properties"));
            assertTrue("Should get time greater than 0",((long) r.get("time")) >= 0);
            sb.append(getDecompressedData(algo, r.get("cypherStatements")));
        });
        assertEquals(EXPECTED_NEO4J_SHELL.replace("LIMIT 20000", "LIMIT 3"), sb.toString());
    }

    // -- Whole file test -- //
    @Test
    public void testExportAllCypherDefault() throws Exception {
        String fileName = "all.cypher";
        TestUtil.testCall(db, "CALL apoc.export.cypher.all($fileName,{useOptimizations: { type: 'none'}, format: 'neo4j-shell'})",
                map("fileName", fileName),
                (r) -> assertResults(fileName, r, "database"));
        assertEquals(EXPECTED_NEO4J_SHELL, readFile(fileName));
    }

    @Test
    public void testExportAllCypherForCypherShell() throws Exception {
        String fileName = "all.cypher";
        TestUtil.testCall(db, "CALL apoc.export.cypher.all($file,$config)",
                map("file", fileName, "config", map("useOptimizations", map("type", "none"), "format", "cypher-shell")),
                (r) -> assertResults(fileName, r, "database"));
        assertEquals(EXPECTED_CYPHER_SHELL, readFile(fileName));
    }

    @Test
    public void testExportQueryCypherForNeo4j() throws Exception {
        String fileName = "all.cypher";
        String query = "MATCH (n) OPTIONAL MATCH p = (n)-[r]-(m) RETURN n,r,m";
        TestUtil.testCall(db, exportQuery,
                map("file", fileName, "query", query, "config", map("useOptimizations", map("type", "none"), "format", "neo4j-shell")), (r) -> {
                });
        assertEquals(EXPECTED_NEO4J_SHELL, readFile(fileName));
    }

    private static String readFile(String fileName) {
        return readFile(fileName, CompressionAlgo.NONE);
    }

    private static String readFile(String fileName, CompressionAlgo algo) {
        return BinaryTestUtil.readFileToString(new File(directory, fileName), UTF_8, algo);
    }

    @Test
    public void testExportQueryOnlyRel() {
        final Map<String, Object> config = withoutOptimization( map("format", "neo4j-shell") );
        Map<String, Object> params = Map.of("query", queryWithRelOnly,
                "config", config);
        assertExportRelOnly(params, exportQuery);

        // check that apoc.export.cypher.data returns consistent results
        assertExportRelOnly(params, exportDataWithRelOnly);
    }

    @Test
    public void testExportQueryOnlyRelWithNodeOfRelsTrue() {
        // check that `nodesOfRelationships: true` doesn't change the result
        final Map<String, Object> config = withoutOptimization(
                map("format", "neo4j-shell",
                        "nodesOfRelationships", true)
        );
        Map<String, Object> params = Map.of("query", queryWithRelOnly,
                "config", config);
        assertExportRelOnly(params, exportQuery);

        // check that apoc.export.cypher.data returns consistent results
        assertExportRelOnly(params, exportDataWithRelOnly);
    }

    @Test
    public void testExportQueryOnlyRelAndStart() {
        final Map<String, Object> config = withoutOptimization( map("format", "neo4j-shell") );
        Map<String, Object> params = Map.of("query", queryWithStartAndRel,
                "config", config);
        assertExportWithoutEndNode(params, exportQuery);

        // check that apoc.export.cypher.data returns consistent results
        assertExportWithoutEndNode(params, exportDataWithStartAndRel);
    }

    @Test
    public void testExportQueryOnlyRelAndStartWithNodeOfRelsTrue() {
        // check that with {nodesOfRelationships: true} the end node is returned as well
        final Map<String, Object> config = withoutOptimization(
                map("format", "neo4j-shell",
                        "nodesOfRelationships", true)
        );
        Map<String, Object> params = Map.of("query", queryWithStartAndRel,
                "config", config);

        String expectedNodesWithEndNode = String.format(EXPECTED_BEGIN_AND_FOO +
                "CREATE (:Bar:`UNIQUE IMPORT LABEL` {age:42, name:\"bar\", `UNIQUE IMPORT ID`:1});%n" +
                "COMMIT%n");
        String expectedWithEndNode = expectedNodesWithEndNode + EXPECTED_SCHEMA_ONLY_START + EXPECTED_REL_ONLY + EXPECTED_CLEAN_UP;

        commonDataAndQueryAssertions(exportQuery, params,
                expectedWithEndNode, 2L, 1L, 5L);

        // apoc.export.cypher.data doesn't accept `nodesOfRelationships` config,
        // so it returns the same result as the above one
        assertExportWithoutEndNode(params, exportDataWithStartAndRel);
    }

    private void assertExportWithoutEndNode(Map<String, Object> params, String exportData) {
        commonDataAndQueryAssertions(exportData, params,
                EXPECTED_WITHOUT_END_NODE, 1L, 1L, 3L);
    }

    private void assertExportRelOnly(Map<String, Object> params, String exportData) {
        commonDataAndQueryAssertions(exportData, params,
                EXPECTED_REL_ONLY, 0L, 1L, 1L);
    }

    private void commonDataAndQueryAssertions(String query, Map<String, Object> config,
                                              String expectedOutput, long nodes, long relationships, long properties) {
        String fileName = "testFile.cypher";

        Map<String, Object> params = map("file", fileName);
        params.putAll(config);

        TestUtil.testCall(db, query, params, r -> {
                    assertEquals(nodes, r.get("nodes"));
                    assertEquals(relationships, r.get("relationships"));
                    assertEquals(properties, r.get("properties"));
                });
        assertEquals(expectedOutput, readFile(fileName));
    }


    @Test
    public void testExportCypherAdminOperationErrorMessage() {
        String filename = "test.cypher";
        List<String> invalidQueries = List.of(
                "SHOW CONSTRAINTS YIELD id, name, type RETURN *",
                "SHOW INDEXES YIELD id, name, type RETURN *"
        );

        for (String query : invalidQueries) {
            QueryExecutionException e = Assert.assertThrows(QueryExecutionException.class,
                    () -> TestUtil.testCall(db, exportQuery,
                            MapUtil.map(
                                    "query", query,
                                    "file", filename,
                                    "config", map("useOptimizations", map("type", "none"), "format", "neo4j-shell")),
                            (r) -> assertResults(filename, r, "database")
                    )
            );

            assertError(e, INVALID_QUERY_MODE_ERROR, RuntimeException.class, "apoc.export.cypher.query");
        }
    }


    @Test
    public void testExportGraphCypher() throws Exception {
        String fileName = "graph.cypher";
        TestUtil.testCall(db, "CALL apoc.graph.fromDB('test',{}) yield graph " +
                "CALL apoc.export.cypher.graph(graph, $file,$exportConfig) " +
                "YIELD nodes, relationships, properties, file, source,format, time " +
                "RETURN *", map("file", fileName, "exportConfig", map("useOptimizations", map("type", "none"), "format", "neo4j-shell")),
                (r) -> assertResults(fileName, r, "graph"));
        assertEquals(EXPECTED_NEO4J_SHELL, readFile(fileName));
    }

    // -- Separate files tests -- //
    @Test
    public void testExportAllCypherNodes() throws Exception {
        String fileName = "all.cypher";
        TestUtil.testCall(db, "CALL apoc.export.cypher.all($file,$exportConfig)", map("file", fileName, "exportConfig", exportConfig),
                (r) -> assertResults(fileName, r, "database"));
        assertEquals(EXPECTED_NODES, readFile("all.nodes.cypher"));
    }

    @Test
    public void testExportAllCypherRelationships() throws Exception {
        String fileName = "all.cypher";
        TestUtil.testCall(db, "CALL apoc.export.cypher.all($file,$exportConfig)", map("file", fileName, "exportConfig", exportConfig),
                (r) -> assertResults(fileName, r, "database"));
        assertEquals(EXPECTED_RELATIONSHIPS, readFile("all.relationships.cypher"));
    }

    @Test
    public void testExportAllCypherSchema() throws Exception {
        String fileName = "all.cypher";
        TestUtil.testCall(db, "CALL apoc.export.cypher.all($file,$exportConfig)", map("file", fileName, "exportConfig", exportConfig),
                (r) -> assertResults(fileName, r, "database"));
        assertEquals(EXPECTED_SCHEMA, readFile("all.schema.cypher"));
    }

    @Test
    public void testExportAllCypherSchemaWithSaveIdxNames() throws Exception {
        final Map<String, Object> config = new HashMap<>(ExportCypherTest.exportConfig);
        config.put("saveIndexNames", true);
        String fileName = "all.cypher";
        TestUtil.testCall(db, "CALL apoc.export.cypher.all($file,$config)", 
                map("file", fileName, "config", config),
                (r) -> assertResults(fileName, r, "database"));
        final String expectedFile = String.format(EXPECTED_SCHEMA_WITH_NAMES, " barIndex", " fooIndex", StringUtils.EMPTY, StringUtils.EMPTY);
        assertEquals(expectedFile, readFile("all.schema.cypher"));
    }

    @Test
    public void testExportAllCypherSchemaWithSaveConstraintNames() throws Exception {
        final Map<String, Object> config = new HashMap<>(ExportCypherTest.exportConfig);
        config.put("saveConstraintNames", true);
        String fileName = "all.cypher";
        TestUtil.testCall(db, "CALL apoc.export.cypher.all($file,$config)",
                map("file", fileName, "config", config),
                (r) -> assertResults(fileName, r, "database"));
        final String expectedFile = String.format(EXPECTED_SCHEMA_WITH_NAMES, StringUtils.EMPTY, StringUtils.EMPTY, " consBar", " uniqueConstraintComposite");
        assertEquals(expectedFile, readFile("all.schema.cypher"));
    }

    @Test
    public void testExportAllCypherCleanUp() throws Exception {
        String fileName = "all.cypher";
        TestUtil.testCall(db, "CALL apoc.export.cypher.all($file,$exportConfig)", map("file", fileName, "exportConfig", exportConfig),
                (r) -> assertResults(fileName, r, "database"));
        assertEquals(EXPECTED_CLEAN_UP, readFile("all.cleanup.cypher"));
    }

    @Test
    public void testExportGraphCypherNodes() throws Exception {
        String fileName = "graph.cypher";
        TestUtil.testCall(db, "CALL apoc.graph.fromDB('test',{}) yield graph " +
                "CALL apoc.export.cypher.graph(graph, $file,$exportConfig) " +
                "YIELD nodes, relationships, properties, file, source,format, time " +
                "RETURN *", map("file", fileName, "exportConfig", exportConfig), (r) -> assertResults(fileName, r, "graph"));
        assertEquals(EXPECTED_NODES, readFile("graph.nodes.cypher"));
    }

    @Test
    public void testExportGraphCypherRelationships() throws Exception {
        String fileName = "graph.cypher";
        TestUtil.testCall(db, "CALL apoc.graph.fromDB('test',{}) yield graph " +
                        "CALL apoc.export.cypher.graph(graph, $file,$exportConfig) " +
                        "YIELD nodes, relationships, properties, file, source,format, time " +
                        "RETURN *", map("file", fileName, "exportConfig", exportConfig),
                (r) -> assertResults(fileName, r, "graph"));
        assertEquals(EXPECTED_RELATIONSHIPS, readFile("graph.relationships.cypher"));
    }

    @Test
    public void testExportGraphCypherSchema() throws Exception {
        String fileName = "graph.cypher";
        TestUtil.testCall(db, "CALL apoc.graph.fromDB('test',{}) yield graph " +
                        "CALL apoc.export.cypher.graph(graph, $file,$exportConfig) " +
                        "YIELD nodes, relationships, properties, file, source,format, time " +
                        "RETURN *", map("file", fileName, "exportConfig", exportConfig),
                (r) -> assertResults(fileName, r, "graph"));
        assertEquals(EXPECTED_SCHEMA, readFile("graph.schema.cypher"));
    }

    @Test
    public void testExportGraphCypherCleanUp() throws Exception {
        String fileName = "graph.cypher";
        TestUtil.testCall(db, "CALL apoc.graph.fromDB('test',{}) yield graph " +
                        "CALL apoc.export.cypher.graph(graph, $file,$exportConfig) " +
                        "YIELD nodes, relationships, properties, file, source,format, time " +
                        "RETURN *", map("file", fileName, "exportConfig", exportConfig),
                (r) -> assertResults(fileName, r, "graph"));
        assertEquals(EXPECTED_CLEAN_UP, readFile("graph.cleanup.cypher"));
    }

    private void assertResults(String fileName, Map<String, Object> r, final String source) {
        assertEquals(3L, r.get("nodes"));
        assertEquals(1L, r.get("relationships"));
        assertEquals(6L, r.get("properties"));
        assertEquals(fileName, r.get("file"));
        assertEquals(source + ": nodes(3), rels(1)", r.get("source"));
        assertEquals("cypher", r.get("format"));
        assertTrue("Should get time greater than 0",((long) r.get("time")) >= 0);
    }

    @Test
    public void testExportQueryCypherPlainFormat() throws Exception {
        String fileName = "all.cypher";
        String query = "MATCH (n) OPTIONAL MATCH p = (n)-[r]-(m) RETURN n,r,m";
        TestUtil.testCall(db, exportQuery,
                map("file", fileName, "query", query, "config", map("useOptimizations", map("type", "none"), "format", "plain")), (r) -> {
                });
        assertEquals(EXPECTED_PLAIN, readFile(fileName));
    }

    @Test
    public void testExportQueryCypherFormatUpdateAll() throws Exception {
        String fileName = "all.cypher";
        String query = "MATCH (n) OPTIONAL MATCH p = (n)-[r]-(m) RETURN n,r,m";
        TestUtil.testCall(db, exportQuery,
                map("file", fileName, "query", query, "config", map("useOptimizations", map("type", "none"), "format", "neo4j-shell", "cypherFormat", "updateAll")), (r) -> {
                });
        assertEquals(EXPECTED_NEO4J_MERGE, readFile(fileName));
    }

    @Test
    public void testExportQueryCypherFormatAddStructure() throws Exception {
        String fileName = "all.cypher";
        String query = "MATCH (n) OPTIONAL MATCH p = (n)-[r]-(m) RETURN n,r,m";
        TestUtil.testCall(db, exportQuery,
                map("file", fileName, "query", query, "config", map("useOptimizations", map("type", "none"), "format", "neo4j-shell", "cypherFormat", "addStructure")), (r) -> {
                });
        assertEquals(EXPECTED_NODES_MERGE_ON_CREATE_SET + EXPECTED_SCHEMA_EMPTY + EXPECTED_RELATIONSHIPS + EXPECTED_CLEAN_UP_EMPTY, readFile(fileName));
    }

    @Test
    public void testExportQueryCypherFormatUpdateStructure() throws Exception {
        String fileName = "all.cypher";
        String query = "MATCH (n) OPTIONAL MATCH p = (n)-[r]-(m) RETURN n,r,m";
        TestUtil.testCall(db, exportQuery,
                map("file", fileName, "query", query, "config", map("useOptimizations", map("type", "none"), "format", "neo4j-shell", "cypherFormat", "updateStructure")), (r) -> {
                });
        assertEquals(EXPECTED_NODES_EMPTY + EXPECTED_SCHEMA_EMPTY + EXPECTED_RELATIONSHIPS_MERGE_ON_CREATE_SET + EXPECTED_CLEAN_UP_EMPTY, readFile(fileName));
    }

    @Test
    public void testExportSchemaCypher() throws Exception {
        String fileName = "onlySchema.cypher";
        TestUtil.testCall(db, "CALL apoc.export.cypher.schema($file,$exportConfig)", map("file", fileName, "exportConfig", exportConfig), (r) -> {
        });
        assertEquals(EXPECTED_ONLY_SCHEMA_NEO4J_SHELL, readFile(fileName));
    }

    @Test
    public void testExportSchemaCypherWithStreamTrue() {
        Consumer<Map<String, Object>> resultAssertion = (r) -> {
            Object actual = r.get("cypherStatements");
            assertEquals(EXPECTED_ONLY_SCHEMA_CYPHER_SHELL, actual);
        };

        // with {stream: true}
        TestUtil.testCall(db, "CALL apoc.export.cypher.schema(null, {stream: true})",
                resultAssertion);

        // with {streamStatements: true}
        TestUtil.testCall(db, "CALL apoc.export.cypher.schema(null, {streamStatements: true})",
                resultAssertion);

        // assert `schemaStatements` of `apoc.export.cypher.all`
        // is equal to `cypher.schema` output plus the `CREATE CONSTRAINT UNIQUE_IMPORT_NAME ...` statement
        TestUtil.testCall(db, "CALL apoc.export.cypher.all(null, { separateFiles: true, stream: true})",
                r -> {
                    Object actual = r.get("schemaStatements");
                    assertEquals(convertToCypherShellFormat(EXPECTED_SCHEMA), actual);
                });
    }

    @Test
    public void testExportSchemaCypherWithIdxAndConsNames() throws Exception {
        final Map<String, Object> config = new HashMap<>(ExportCypherTest.exportConfig);
        config.putAll(map("saveConstraintNames", true, "saveIndexNames", true));
        String fileName = "onlySchema.cypher";
        TestUtil.testCall(db, "CALL apoc.export.cypher.schema($file,$config)", 
                map("file", fileName, "config", config), (r) -> {});
        assertEquals(EXPECTED_ONLY_SCHEMA_NEO4J_SHELL_WITH_NAMES, readFile(fileName));
    }

    @Test
    public void testExportSchemaCypherShell() throws Exception {
        String fileName = "onlySchema.cypher";
        TestUtil.testCall(db, "CALL apoc.export.cypher.schema($file,$exportConfig)",
                map("file", fileName, "exportConfig", map("useOptimizations", map("type", "none"), "format", "cypher-shell")),
                (r) -> {});
        assertEquals(EXPECTED_ONLY_SCHEMA_CYPHER_SHELL, readFile(fileName));
    }

    @Test
    public void testExportCypherNodePoint() throws FileNotFoundException {
        db.executeTransactionally("CREATE (f:Test {name:'foo'," +
                "place2d:point({ x: 2.3, y: 4.5 })," +
                "place3d1:point({ x: 2.3, y: 4.5 , z: 1.2})})" +
                "-[:FRIEND_OF {place2d:point({ longitude: 56.7, latitude: 12.78 })}]->" +
                "(:Bar {place3d:point({ longitude: 12.78, latitude: 56.7, height: 100 })})");
        String fileName = "temporalPoint.cypher";
        String query = "MATCH (n:Test)-[r]-(m) RETURN n,r,m";
        TestUtil.testCall(db, exportQuery,
                map("file", fileName, "query", query, "config", map("useOptimizations", map("type", "none"),"format", "neo4j-shell")),
                (r) -> {});
        assertEquals(EXPECTED_CYPHER_POINT, readFile(fileName));
    }

    @Test
    public void testExportCypherNodeDate() throws FileNotFoundException {
        db.executeTransactionally("CREATE (f:Test {name:'foo', " +
                "date:date('2018-10-30'), " +
                "datetime:datetime('2018-10-30T12:50:35.556+0100'), " +
                "localTime:localdatetime('20181030T19:32:24')})" +
                "-[:FRIEND_OF {date:date('2018-10-30')}]->" +
                "(:Bar {datetime:datetime('2018-10-30T12:50:35.556')})");
        String fileName = "temporalDate.cypher";
        String query = "MATCH (n:Test)-[r]-(m) RETURN n,r,m";
        TestUtil.testCall(db, exportQuery,
                map("file", fileName, "query", query, "config", map("useOptimizations", map("type", "none"),"format", "neo4j-shell")),
                (r) -> {});
        assertEquals(EXPECTED_CYPHER_DATE, readFile(fileName));
    }

    @Test
    public void testExportCypherNodeTime() throws FileNotFoundException {
        db.executeTransactionally("CREATE (f:Test {name:'foo', " +
                "local:localtime('12:50:35.556')," +
                "t:time('125035.556+0100')})" +
                "-[:FRIEND_OF {t:time('125035.556+0100')}]->" +
                "(:Bar {datetime:datetime('2018-10-30T12:50:35.556+0100')})");
        String fileName = "temporalTime.cypher";
        String query = "MATCH (n:Test)-[r]-(m) RETURN n,r,m";
        TestUtil.testCall(db, exportQuery,
                map("file", fileName, "query", query, "config", map("useOptimizations", map("type", "none"),"format", "neo4j-shell")),
                (r) -> {});
        assertEquals(EXPECTED_CYPHER_TIME, readFile(fileName));
    }

    @Test
    public void testExportCypherNodeDuration() throws FileNotFoundException {
        db.executeTransactionally("CREATE (f:Test {name:'foo', " +
                "duration:duration('P5M1.5D')})" +
                "-[:FRIEND_OF {duration:duration('P5M1.5D')}]->" +
                "(:Bar {duration:duration('P5M1.5D')})");
        String fileName = "temporalDuration.cypher";
        String query = "MATCH (n:Test)-[r]-(m) RETURN n,r,m";
        TestUtil.testCall(db, exportQuery,
                map("file", fileName, "query", query, "config", map("useOptimizations", map("type", "none"),"format", "neo4j-shell")),
                (r) -> {});
        assertEquals(EXPECTED_CYPHER_DURATION, readFile(fileName));
    }

    @Test
    public void testExportWithAscendingLabels() throws FileNotFoundException {
        db.executeTransactionally("CREATE (f:User:User1:User0:User12 {name:'Alan'})");
        String fileName = "ascendingLabels.cypher";
        String query = "MATCH (f:User) WHERE f.name='Alan' RETURN f";
        TestUtil.testCall(db, exportQuery,
                map("file", fileName, "query", query, "config", map("useOptimizations", map("type", "none"),"format", "neo4j-shell")),
                (r) -> {});
        assertEquals(EXPECTED_CYPHER_LABELS_ASCENDEND, readFile(fileName));
    }

    @Test
    public void testExportAllCypherDefaultWithUnwindBatchSizeOptimized() throws Exception {
        String fileName = "allDefaultOptimized.cypher";
        TestUtil.testCall(db, "CALL apoc.export.cypher.all($file,{useOptimizations: { type: 'unwind_batch', unwindBatchSize: 2}, format: 'neo4j-shell'})", map("file", fileName),
                (r) -> assertResultsOptimized(fileName, r));
        assertEquals(EXPECTED_NEO4J_OPTIMIZED_BATCH_SIZE, readFile(fileName));
    }

    @Test
    public void testExportAllCypherDefaultOptimized() throws Exception {
        String fileName = "allDefaultOptimized.cypher";
        TestUtil.testCall(db, "CALL apoc.export.cypher.all($file, $exportConfig)", map("file", fileName, "exportConfig", map("format", "neo4j-shell")),
                (r) -> assertResultsOptimized(fileName, r));
        assertEquals(EXPECTED_NEO4J_OPTIMIZED, readFile(fileName));
    }

    @Test
    public void testExportAllCypherDefaultSeparatedFilesOptimized() throws Exception {
        String fileName = "allDefaultOptimized.cypher";
        TestUtil.testCall(db, "CALL apoc.export.cypher.all($file, $exportConfig)",
                map("file", fileName, "exportConfig", map("separateFiles", true, "format", "neo4j-shell")),
                (r) -> assertResultsOptimized(fileName, r));
        assertEquals(EXPECTED_NODES_OPTIMIZED, readFile("allDefaultOptimized.nodes.cypher"));
        assertEquals(EXPECTED_RELATIONSHIPS_OPTIMIZED, readFile("allDefaultOptimized.relationships.cypher"));
        assertEquals(EXPECTED_SCHEMA_OPTIMIZED, readFile("allDefaultOptimized.schema.cypher"));
        assertEquals(EXPECTED_CLEAN_UP, readFile("allDefaultOptimized.cleanup.cypher"));
    }

    @Test
    public void testExportAllCypherWithIfNotExistsFalseOptimized() throws Exception {
        String fileName = "ifNotExists.cypher";
        TestUtil.testCall(db, "CALL apoc.export.cypher.all($file, $config)",
                map("file", fileName, "config", map("ifNotExists", true, "separateFiles", true, "format", "neo4j-shell")),
                (r) -> assertResultsOptimized(fileName, r));
        assertEquals(EXPECTED_SCHEMA_OPTIMIZED_WITH_IF_NOT_EXISTS, readFile("ifNotExists.schema.cypher"));
        assertEquals(EXPECTED_NODES_OPTIMIZED, readFile("ifNotExists.nodes.cypher"));
        assertEquals(EXPECTED_RELATIONSHIPS_OPTIMIZED, readFile("ifNotExists.relationships.cypher"));
        assertEquals(EXPECTED_CLEAN_UP, readFile("ifNotExists.cleanup.cypher"));
    }

    @Test
    public void testExportAllCypherCypherShellWithUnwindBatchSizeOptimized() throws Exception {
        String fileName = "allCypherShellOptimized.cypher";
        TestUtil.testCall(db, "CALL apoc.export.cypher.all($file,{format:'cypher-shell', useOptimizations: {type: 'unwind_batch'}})",
                map("file", fileName),
                (r) -> assertResultsOptimized(fileName, r));
        assertEquals(EXPECTED_CYPHER_SHELL_OPTIMIZED_BATCH_SIZE, readFile(fileName));
    }

    @Test
    public void testExportAllCypherCypherShellOptimized() throws Exception {
        String fileName = "allCypherShellOptimized.cypher";
        TestUtil.testCall(db, "CALL apoc.export.cypher.all($file,{format:'cypher-shell'})",
                map("file", fileName),
                (r) -> assertResultsOptimized(fileName, r));
        assertEquals(EXPECTED_CYPHER_SHELL_OPTIMIZED, readFile(fileName));
    }

    @Test
    public void testExportAllCypherPlainWithUnwindBatchSizeOptimized() throws Exception {
        String fileName = "allPlainOptimized.cypher";
        TestUtil.testCall(db, "CALL apoc.export.cypher.all($file,{format:'plain', useOptimizations: { type: 'unwind_batch', unwindBatchSize: 2}})",
                map("file", fileName),
                (r) -> assertResultsOptimized(fileName, r));
        assertEquals(EXPECTED_PLAIN_OPTIMIZED_BATCH_SIZE, readFile(fileName));
    }

    @Test
    public void testExportAllCypherPlainAddStructureWithUnwindBatchSizeOptimized() throws Exception {
        String fileName = "allPlainAddStructureOptimized.cypher";
        TestUtil.testCall(db, "CALL apoc.export.cypher.all($file,{format:'plain', cypherFormat: 'addStructure', useOptimizations: { type: 'unwind_batch', unwindBatchSize: 2}})",
                map("file", fileName), (r) -> assertResultsOptimized(fileName, r));
        assertEquals(EXPECTED_PLAIN_ADD_STRUCTURE_UNWIND, readFile(fileName));
    }

    @Test
    public void testExportAllCypherPlainUpdateStructureWithUnwindBatchSizeOptimized() throws Exception {
        String fileName = "allPlainUpdateStructureOptimized.cypher";
        TestUtil.testCall(db, "CALL apoc.export.cypher.all($file,{format:'plain', cypherFormat: 'updateStructure', useOptimizations: { type: 'unwind_batch', unwindBatchSize: 2}})",
                map("file", fileName), (r) -> {
                    assertEquals(0L, r.get("nodes"));
                    assertEquals(2L, r.get("relationships"));
                    assertEquals(2L, r.get("properties"));
                    assertEquals(fileName, r.get("file"));
                    assertEquals("cypher", r.get("format"));
                    assertTrue("Should get time greater than 0",((long) r.get("time")) >= 0);
                });
        assertEquals(EXPECTED_PLAIN_UPDATE_STRUCTURE_UNWIND, readFile(fileName));
    }

    @Test
    public void testExportAllCypherPlainUpdateAllWithUnwindBatchSizeOptimized() throws Exception {
        String fileName = "allPlainUpdateAllOptimized.cypher";
        TestUtil.testCall(db, "CALL apoc.export.cypher.all($file,{format:'plain', cypherFormat: 'updateAll', useOptimizations: { type: 'unwind_batch', unwindBatchSize: 2}})",
                map("file", fileName), (r) -> assertResultsOptimized(fileName, r));
        assertEquals(EXPECTED_UPDATE_ALL_UNWIND, readFile(fileName));
    }

    @Test
    public void testExportQueryCypherShellWithUnwindBatchSizeWithBatchSizeOptimized() throws Exception {
        String fileName = "allPlainOptimized.cypher";
        TestUtil.testCall(db, "CALL apoc.export.cypher.all($file,{format:'cypher-shell', useOptimizations: { type: 'unwind_batch', unwindBatchSize: 2}, batchSize: 2})",
                map("file", fileName),
                (r) -> assertResultsOptimized(fileName, r));
        assertEquals(EXPECTED_QUERY_CYPHER_SHELL_OPTIMIZED_UNWIND, readFile(fileName));
    }

    @Test
    public void testExportQueryCypherShellWithCompressionWithUnwindBatchSizeWithBatchSizeOptimized() throws Exception {
        final CompressionAlgo algo = CompressionAlgo.DEFLATE;
        String fileName = "allPlainOptimized.cypher.ZZ";
        TestUtil.testCall(db, "CALL apoc.export.cypher.all($file,{compression: '" + algo.name() + "', format:'cypher-shell', useOptimizations: { type: 'unwind_batch', unwindBatchSize: 2}, batchSize: 2})",
                map("file", fileName),
                (r) -> assertResultsOptimized(fileName, r));
        assertEquals(EXPECTED_QUERY_CYPHER_SHELL_OPTIMIZED_UNWIND, readFile(fileName, algo));
    }

    @Test
    public void testExportQueryCypherShellWithUnwindBatchSizeWithBatchSizeOddDataset() throws Exception {
        String fileName = "allPlainOdd.cypher";
        TestUtil.testCall(db, "CALL apoc.export.cypher.all($file,{format:'cypher-shell', useOptimizations: { type: 'unwind_batch', unwindBatchSize: 2}, batchSize: 2})",
                map("file", fileName), (r) -> assertResultsOdd(fileName, r));
        assertEquals(EXPECTED_QUERY_CYPHER_SHELL_OPTIMIZED_ODD, readFile(fileName));
    }

    @Test
    public void testExportQueryCypherShellUnwindBatchParamsWithOddDataset() throws Exception {
        String fileName = "allPlainOdd.cypher";
        TestUtil.testCall(db, "CALL apoc.export.cypher.all($file,{format:'cypher-shell', useOptimizations: { type: 'unwind_batch_params', unwindBatchSize: 2}, batchSize:2})",
                map("file", fileName),
                (r) -> assertResultsOdd(fileName, r));
        assertEquals(EXPECTED_QUERY_CYPHER_SHELL_PARAMS_OPTIMIZED_ODD, readFile(fileName));
    }
    
    @Test
    public void testExportWithCompressionQueryCypherShellUnwindBatchParamsWithOddDataset() {
        final CompressionAlgo algo = CompressionAlgo.BZIP2;
        String fileName = "allPlainOdd.cypher.bz2";
        TestUtil.testCall(db, "CALL apoc.export.cypher.all($file,{compression: '" + algo.name() + "', format:'cypher-shell', useOptimizations: { type: 'unwind_batch_params', unwindBatchSize: 2}, batchSize:2})",
                map("file", fileName),
                (r) -> assertResultsOdd(fileName, r));
        assertEquals(EXPECTED_QUERY_CYPHER_SHELL_PARAMS_OPTIMIZED_ODD, readFile(fileName, algo));
    }

    @Test
    @Ignore("non-deterministic index order")
    public void testExportAllCypherPlainOptimized() throws Exception {
        String fileName = "queryPlainOptimized.cypher";
        TestUtil.testCall(db, "CALL apoc.export.cypher.query('MATCH (f:Foo)-[r:KNOWS]->(b:Bar) return f,r,b', $file,{format:'cypher-shell', useOptimizations: {type: 'unwind_batch'}})",
                map("file", fileName),
                (r) -> {
                    assertEquals(4L, r.get("nodes"));
                    assertEquals(2L, r.get("relationships"));
                    assertEquals(10L, r.get("properties"));
                    assertEquals(fileName, r.get("file"));
                    assertEquals("statement: nodes(4), rels(2)", r.get("source"));
                    assertEquals("cypher", r.get("format"));
                    assertTrue("Should get time greater than 0",((long) r.get("time")) >= 0);
                });
        String actual = readFile(fileName);
        assertTrue("expected generated output",EXPECTED_QUERY_CYPHER_SHELL_OPTIMIZED.equals(actual) || EXPECTED_QUERY_CYPHER_SHELL_OPTIMIZED2.equals(actual));
    }

    @Test
    public void testExportQueryCypherShellUnwindBatchParamsWithOddBatchSizeOddDataset() throws Exception {
        db.executeTransactionally("CREATE (:Bar {name:'bar3',age:35}), (:Bar {name:'bar4',age:36})");
        String fileName = "allPlainOddNew.cypher";
        TestUtil.testCall(db, "CALL apoc.export.cypher.all($file,{format:'cypher-shell', useOptimizations: { type: 'unwind_batch_params', unwindBatchSize: 2}, batchSize:3})",
                map("file", fileName),
                (r) -> {});
        db.executeTransactionally("MATCH (n:Bar {name:'bar3',age:35}), (n1:Bar {name:'bar4',age:36}) DELETE n, n1");
        assertEquals(EXPECTED_QUERY_PARAMS_ODD, readFile(fileName));
    }

    @Test
    public void exportMultiTokenIndex() {
        // given
        db.executeTransactionally("CREATE (n:TempNode {value:'value', value2:'value'})");
        db.executeTransactionally("CREATE (n:TempNode2 {value:'value', value:'value2'})");
        db.executeTransactionally("CALL db.index.fulltext.createNodeIndex('MyCoolNodeFulltextIndex',['TempNode', 'TempNode2'],['value', 'value2'])");

        String query = "MATCH (t:TempNode) return t";
        String file = null;
        Map<String, Object> config = map("awaitForIndexes", 3000);
        String expected = String.format(":begin%n" +
                "CREATE FULLTEXT INDEX MyCoolNodeFulltextIndex FOR (n:TempNode|TempNode2) ON EACH [n.value, n.value2];%n" +
                "CREATE CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT (node.`UNIQUE IMPORT ID`) IS UNIQUE;%n" +
                ":commit%n" +
                "CALL db.awaitIndexes(3000);%n" +
                ":begin%n" +
                "UNWIND [{_id:3, properties:{value2:\"value\", value:\"value\"}}] AS row%n" +
                "CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:TempNode;%n" +
                ":commit%n" +
                ":begin%n" +
                "MATCH (n:`UNIQUE IMPORT LABEL`)  WITH n LIMIT 20000 REMOVE n:`UNIQUE IMPORT LABEL` REMOVE n.`UNIQUE IMPORT ID`;%n" +
                ":commit%n" +
                ":begin%n" +
                "DROP CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT (node.`UNIQUE IMPORT ID`) IS UNIQUE;%n" +
                ":commit%n");

        // when
        TestUtil.testCall(db, exportQuery,
                map("query", query, "file", file, "config", config),
                (r) -> {
                    // then
                    assertEquals(expected, r.get("cypherStatements"));
                });
    }

    @Ignore("It doesn't fail anymore because it skips not supported indexes")
    @Test(expected = QueryExecutionException.class)
    public void shouldFailExportMultiTokenIndexForRelationship() {
        // given
        db.executeTransactionally("CREATE (n:TempNode {value:'value'})");
        db.executeTransactionally("CREATE (n:TempNode2 {value:'value'})");
        db.executeTransactionally("CALL db.index.fulltext.createNodeIndex('MyCoolNodeFulltextIndex',['TempNode', 'TempNode2'],['value'])");

        // TODO: We can't manage full-text rel indexes because of this bug: https://github.com/neo4j/neo4j/issues/12304
        db.executeTransactionally("CREATE (s:TempNode)-[:REL{rel_value: 'the rel value'}]->(e:TempNode2)");
        db.executeTransactionally("CALL db.index.fulltext.createRelationshipIndex('MyCoolRelFulltextIndex',['REL'],['rel_value'])");
        String query = "MATCH (t:TempNode) return t";
        String file = null;
        Map<String, Object> config = map("awaitForIndexes", 3000);

        try {
            // when
            TestUtil.testCall(db, "CALL apoc.export.cypher.query($query, $file, $config)",
                    map("query", query, "file", file, "config", config),
                    (r) -> {});
        } catch (Exception e) {
            String expected = "Full-text indexes on relationships are not supported, please delete them in order to complete the process";
            assertEquals(expected, ExceptionUtils.getRootCause(e).getMessage());
            throw e;
        }
    }

    @Test
    public void shouldNotCreateUniqueImportIdForUniqueConstraint() {
        db.executeTransactionally("CREATE (n:Bar:Baz{name: 'A'})");
        String query = "MATCH (n:Baz) RETURN n";
        /* The bug was:
        UNWIND [{name:"A", _id:20, properties:{}}] AS row
        CREATE (n:Bar{name: row.name, `UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Baz;
        But should be the expected variable
         */
        final String expected = "UNWIND [{name:\"A\", properties:{}}] AS row\n" +
                "CREATE (n:Bar{name: row.name}) SET n += row.properties SET n:Baz";
        TestUtil.testCall(db, exportQuery,
                map("file", null, "query", query, "config", map("format", "plain", "stream", true)), (r) -> {
                    final String cypherStatements = (String) r.get("cypherStatements");
                    String unwind = Stream.of(cypherStatements.split(";"))
                            .map(String::trim)
                            .filter(s -> s.startsWith("UNWIND"))
                            .findFirst()
                            .orElse(null);
                    assertEquals(expected, unwind);
                });
    }

    @Test
    public void shouldManageBigNumbersCorrectly() {
        db.executeTransactionally("MATCH (n) DETACH DELETE n");
        db.executeTransactionally("CREATE (:Bar{name:'Foo', var1:1.416785E32}), (:Bar{name:'Bar', var1:12E4});");
        final String expected = "UNWIND [{name:\"Foo\", properties:{var1:1.416785E32}}, {name:\"Bar\", properties:{var1:120000.0}}] AS row\n" +
                "CREATE (n:Bar{name: row.name}) SET n += row.properties";
        TestUtil.testCall(db, "CALL apoc.export.cypher.all($file, $config)",
                map("file", null, "config", map("format", "plain", "stream", true)), (r) -> {
                    final String cypherStatements = (String) r.get("cypherStatements");
                    String unwind = Stream.of(cypherStatements.split(";"))
                            .map(String::trim)
                            .filter(s -> s.startsWith("UNWIND"))
                            .findFirst()
                            .orElse(null);
                    assertEquals(expected, unwind);
                });
    }
    
    @Test
    public void shouldQuotePropertyNameStartingWithDollarCharacter() {
        db.executeTransactionally("CREATE (n:Bar:Baz{name: 'A', `$lock`: true})");
        String query = "MATCH (n:Baz) RETURN n";
        final String expected = "UNWIND [{name:\"A\", properties:{`$lock`:true}}] AS row\n" +
                "CREATE (n:Bar{name: row.name}) SET n += row.properties SET n:Baz";
        TestUtil.testCall(db, exportQuery,
                map("file", null, "query", query, "config", map("format", "plain", "stream", true)), (r) -> {
                    final String cypherStatements = (String) r.get("cypherStatements");
                    String unwind = Stream.of(cypherStatements.split(";"))
                            .map(String::trim)
                            .filter(s -> s.startsWith("UNWIND"))
                            .findFirst()
                            .orElse(null);
                    assertEquals(expected, unwind);
                });
    }
  
    @Test
    public void shouldHandleTwoLabelsWithOneUniqueConstraintEach() {
        db.executeTransactionally("CREATE CONSTRAINT ON (b:Base) ASSERT b.id IS UNIQUE");
        db.executeTransactionally("CREATE (b:Bar:Base {id:'waBfk3z', name:'barista',age:42})" +
                "-[:KNOWS]->(f:Foo {name:'foosha', born:date('2018-10-31')})");
        String query = "MATCH (b:Bar:Base)-[r:KNOWS]-(f:Foo) RETURN b, f, r";
        /* The bug was:
        UNWIND [{start: {name:"barista"}, end: {_id:4}, properties:{}}] AS row
        MATCH (start:Bar{name: row.start.name, id: row.start.id})    // <-- Notice id here but not above in UNWIND!
        MATCH (end:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row.end._id})
        CREATE (start)-[r:KNOWS]->(end) SET r += row.properties;
        But should be the expected variable
         */
        final String expected = "UNWIND [{start: {name:\"barista\"}, end: {_id:4}, properties:{}}] AS row\n" +
                "MATCH (start:Bar{name: row.start.name})\n" +
                "MATCH (end:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row.end._id})\n" +
                "CREATE (start)-[r:KNOWS]->(end) SET r += row.properties";
        TestUtil.testCall(db, "CALL apoc.export.cypher.query($query, $file, $config)",
                map("file", null, "query", query, "config", map("format", "plain", "stream", true)), (r) -> {
                    final String cypherStatements = (String) r.get("cypherStatements");
                    String unwind = Stream.of(cypherStatements.split(";"))
                            .map(String::trim)
                            .filter(s -> s.startsWith("UNWIND"))
                            .filter(s -> s.contains("barista"))
                            .skip(1)
                            .findFirst()
                            .orElse(null);
                    assertEquals(expected, unwind);
                });
    }

    @Test
    public void shouldHandleTwoLabelsWithTwoUniqueConstraintsEach() {
        db.executeTransactionally("CREATE CONSTRAINT ON (b:Base) ASSERT b.id IS UNIQUE");
        db.executeTransactionally("CREATE CONSTRAINT ON (b:Base) ASSERT b.oid IS UNIQUE");
        db.executeTransactionally("CREATE CONSTRAINT ON (b:Bar) ASSERT b.oname IS UNIQUE");
        db.executeTransactionally("CREATE (b:Bar:Base {id:'waBfk3z',oid:42,name:'barista',oname:'bar',age:42})" +
                "-[:KNOWS]->(f:Foo {name:'foosha', born:date('2018-10-31')})");
        String query = "MATCH (b:Bar:Base)-[r:KNOWS]-(f:Foo) RETURN b, f, r";
        final String expected = "UNWIND [{start: {name:\"barista\"}, end: {_id:4}, properties:{}}] AS row\n" +
                "MATCH (start:Bar{name: row.start.name})\n" +
                "MATCH (end:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row.end._id})\n" +
                "CREATE (start)-[r:KNOWS]->(end) SET r += row.properties";
        TestUtil.testCall(db, exportQuery,
                map("file", null, "query", query, "config", map("format", "plain", "stream", true)), (r) -> {
                    final String cypherStatements = (String) r.get("cypherStatements");
                    String unwind = Stream.of(cypherStatements.split(";"))
                            .map(String::trim)
                            .filter(s -> s.startsWith("UNWIND"))
                            .filter(s -> s.contains("barista"))
                            .skip(1)
                            .findFirst()
                            .orElse(null);
                    assertEquals(expected, unwind);
                });
    }
    
    @Test
    public void shouldSaveCorrectlyRelIndexesOptimized() throws FileNotFoundException {
        String fileName = "relIndex.cypher";
        db.executeTransactionally("CREATE INDEX rel_index_name FOR ()-[r:KNOWS]-() ON (r.since, r.foo)");

        relIndexTestCommon(fileName, EXPECTED_SCHEMA_WITH_RELS_OPTIMIZED, map("ifNotExists", false));

        // with ifNotExists: true
        relIndexTestCommon(fileName, EXPECTED_SCHEMA_WITH_RELS_AND_IF_NOT_EXISTS, map("ifNotExists", true));

        db.executeTransactionally("DROP INDEX rel_index_name");
    }
    
    @Test
    public void shouldSaveCorrectlyRelIndexesWithNameOptimized() throws FileNotFoundException {
        String fileName = "relIndex.cypher";
        db.executeTransactionally("CREATE INDEX rel_index_name FOR ()-[r:KNOWS]-() ON (r.since, r.foo)");

        relIndexTestCommon(fileName, EXPECTED_SCHEMA_WITH_RELS_AND_NAME_OPTIMIZED, map("ifNotExists", false, "saveIndexNames", true, "saveConstraintNames", true));

        // with ifNotExists: true
        relIndexTestCommon(fileName, EXPECTED_SCHEMA_OPTIMIZED_WITH_RELS_IF_NOT_EXISTS_AND_NAME, map("ifNotExists", true, "saveIndexNames", true, "saveConstraintNames", true));

        db.executeTransactionally("DROP INDEX rel_index_name");
    }

    @Test
    public void shouldSaveCorrectlyTypeIndexes() {
        db.executeTransactionally("CREATE INDEX relIdxOne FOR ()-[r:KNOWS]-() ON (r.foo)");
        db.executeTransactionally("CREATE TEXT INDEX relIdxTwo FOR ()-[r:KNOWS_TWO]-() ON (r.foo)");
        db.executeTransactionally("CREATE BTREE INDEX relIdxThree FOR ()-[r:KNOWS_THREE]-() ON (r.foo)");

        db.executeTransactionally("CREATE TEXT INDEX fooIndexText FOR (n:Foo) ON (n.nameOne)");
        db.executeTransactionally("CREATE BTREE INDEX fooIndexRange FOR (n:Foo) ON (n.nameTwo)");


        String expectedSchema = String.format("BEGIN%n" +
                EXPECTED_BAR_FOO_INDEXES +
                "CREATE BTREE INDEX FOR (node:Foo) ON (node.nameTwo);%n" +
                "CREATE TEXT INDEX FOR (node:Foo) ON (node.nameOne);%n" +
                "CREATE BTREE INDEX FOR ()-[rel:KNOWS]-() ON (rel.foo);%n" +
                "CREATE BTREE INDEX FOR ()-[rel:KNOWS_THREE]-() ON (rel.foo);%n" +
                "CREATE TEXT INDEX FOR ()-[rel:KNOWS_TWO]-() ON (rel.foo);%n" +
                EXPECTED_BAR_UNIQUE_CONSTRAINTS +
                "COMMIT%n" +
                "SCHEMA AWAIT%n");

        String expected = EXPECTED_NODES + expectedSchema + EXPECTED_RELATIONSHIPS + EXPECTED_CLEAN_UP;

        TestUtil.testCall(db, "CALL apoc.export.cypher.all(null, $config)",
                map("config", map("useOptimizations", map("type", "none"), "format", "neo4j-shell")),
                (r) -> assertEquals(expected, r.get("cypherStatements"))
        );

        db.executeTransactionally("DROP INDEX relIdxOne");
        db.executeTransactionally("DROP INDEX relIdxTwo");
        db.executeTransactionally("DROP INDEX relIdxThree");
        db.executeTransactionally("DROP INDEX fooIndexText");
        db.executeTransactionally("DROP INDEX fooIndexRange");

    }

    private void relIndexTestCommon(String fileName, String expectedSchema, Map<String, Object> config) throws FileNotFoundException {
        Map<String, Object> exportConfig = map("separateFiles", true, "format", "neo4j-shell");
        exportConfig.putAll(config);
        TestUtil.testCall(db, "CALL apoc.export.cypher.all($file, $exportConfig)",
                map("file", fileName, "exportConfig", exportConfig),
                (r) -> assertResultsOptimized(fileName, r));
        assertEquals(EXPECTED_NODES_OPTIMIZED, readFile("relIndex.nodes.cypher"));
        assertEquals(EXPECTED_RELATIONSHIPS_OPTIMIZED, readFile("relIndex.relationships.cypher"));
        assertEquals(EXPECTED_CLEAN_UP, readFile("relIndex.cleanup.cypher"));
        assertEquals(expectedSchema, readFile("relIndex.schema.cypher"));
    }

    private void assertResultsOptimized(String fileName, Map<String, Object> r) {
        assertEquals(7L, r.get("nodes"));
        assertEquals(2L, r.get("relationships"));
        assertEquals(13L, r.get("properties"));
        assertEquals(fileName, r.get("file"));
        assertEquals("database" + ": nodes(7), rels(2)", r.get("source"));
        assertEquals("cypher", r.get("format"));
        assertTrue("Should get time greater than 0",((long) r.get("time")) >= 0);
    }

    private void assertResultsOdd(String fileName, Map<String, Object> r) {
        assertEquals(7L, r.get("nodes"));
        assertEquals(1L, r.get("relationships"));
        assertEquals(13L, r.get("properties"));
        assertEquals(fileName, r.get("file"));
        assertEquals("database" + ": nodes(7), rels(1)", r.get("source"));
        assertEquals("cypher", r.get("format"));
        assertTrue("Should get time greater than 0",((long) r.get("time")) >= 0);
    }

    private Map<String, Object> withoutOptimization(Map<String, Object> map) {
        map.put("useOptimizations", map("type", ExportConfig.OptimizationType.NONE.name()));
        return map;
    }

    public static class ExportCypherResults {
        static final String EXPECTED_ISOLATED_NODE = "CREATE (:Bar:`UNIQUE IMPORT LABEL` {age:12, `UNIQUE IMPORT ID`:2});\n";
        static final String EXPECTED_BAR_END_NODE = "CREATE (:Bar {age:42, name:\"bar\"});%n";
        static final String EXPECTED_BEGIN_AND_FOO = "BEGIN%n" +
                "CREATE (:Foo:`UNIQUE IMPORT LABEL` {born:date('2018-10-31'), name:\"foo\", `UNIQUE IMPORT ID`:0});%n";


        public static final String EXPECTED_NODES = String.format("BEGIN%n" +
                "CREATE (:Foo:`UNIQUE IMPORT LABEL` {born:date('2018-10-31'), name:\"foo\", `UNIQUE IMPORT ID`:0});%n" +
                "CREATE (:Bar {age:42, name:\"bar\"});%n" +
                "CREATE (:Bar:`UNIQUE IMPORT LABEL` {age:12, `UNIQUE IMPORT ID`:2});%n" +
                "COMMIT%n");

        private static final String EXPECTED_NODES_MERGE = String.format("BEGIN%n" +
                "MERGE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:0}) SET n.name=\"foo\", n.born=date('2018-10-31'), n:Foo;%n" +
                "MERGE (n:Bar{name:\"bar\"}) SET n.age=42;%n" +
                "MERGE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:2}) SET n.age=12, n:Bar;%n" +
                "COMMIT%n");

        public static final String EXPECTED_NODES_MERGE_ON_CREATE_SET =
                EXPECTED_NODES_MERGE.replaceAll(" SET ", " ON CREATE SET ");

        public static final String EXPECTED_NODES_EMPTY = String.format("BEGIN%n" +
                "COMMIT%n");

        private static final String EXPECTED_IDX_FOO = "CREATE BTREE INDEX FOR (node:Foo) ON (node.name);%n";

        private static final String EXPECTED_UNIQUE_IMPORT_CONSTRAINT_AND_AWAIT = "CREATE CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT (node.`UNIQUE IMPORT ID`) IS UNIQUE;%n" +
                "COMMIT%n" +
                "SCHEMA AWAIT%n";

        private static final String EXPECTED_CONSTRAINTS_AND_AWAIT =
                "CREATE CONSTRAINT%s ON (node:Bar) ASSERT (node.name) IS UNIQUE;%n" +
                "CREATE CONSTRAINT%s ON (node:Bar) ASSERT (node.name, node.age) IS UNIQUE;%n" +
                EXPECTED_UNIQUE_IMPORT_CONSTRAINT_AND_AWAIT;

        public static final String EXPECTED_BAR_FOO_INDEXES = "CREATE BTREE INDEX FOR (node:Bar) ON (node.first_name, node.last_name);%n" +
                "CREATE BTREE INDEX FOR (node:Foo) ON (node.name);%n";

        public static final String EXPECTED_BAR_UNIQUE_CONSTRAINTS = "CREATE CONSTRAINT ON (node:Bar) ASSERT (node.name) IS UNIQUE;%n" +
                "CREATE CONSTRAINT ON (node:Bar) ASSERT (node.name, node.age) IS UNIQUE;%n" +
                "CREATE CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT (node.`UNIQUE IMPORT ID`) IS UNIQUE;%n";

        public static final String EXPECTED_SCHEMA = String.format("BEGIN%n" +
                EXPECTED_BAR_FOO_INDEXES +
                EXPECTED_BAR_UNIQUE_CONSTRAINTS +
                "COMMIT%n" +
                "SCHEMA AWAIT%n");

        static final String EXPECTED_SCHEMA_WITH_NAMES = "BEGIN%n" +
                "CREATE BTREE INDEX%s FOR (node:Bar) ON (node.first_name, node.last_name);%n" +
                "CREATE BTREE INDEX%s FOR (node:Foo) ON (node.name);%n" +
                EXPECTED_CONSTRAINTS_AND_AWAIT;

        static final String EXPECTED_SCHEMA_ONLY_START = String.format("BEGIN%n" +
                EXPECTED_IDX_FOO +
                EXPECTED_UNIQUE_IMPORT_CONSTRAINT_AND_AWAIT);
        static final String EXPECTED_REL_ONLY = "BEGIN\n" +
                "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:0}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:1}) CREATE (n1)-[r:KNOWS {since:2016}]->(n2);\n" +
                "COMMIT\n";


        public static final String EXPECTED_SCHEMA_EMPTY = String.format("BEGIN%n" +
                "COMMIT%n" +
                "SCHEMA AWAIT%n");

        public static final String EXPECTED_INDEXES_AWAIT = String.format("CALL db.awaitIndexes(300);%n");

        private static final String EXPECTED_INDEXES_AWAIT_QUERY = String.format("CALL db.awaitIndex(300);%n");

        public static final String EXPECTED_RELATIONSHIPS = String.format("BEGIN%n" +
                "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:0}), (n2:Bar{name:\"bar\"}) CREATE (n1)-[r:KNOWS {since:2016}]->(n2);%n" +
                "COMMIT%n");

        private static final String EXPECTED_RELATIONSHIPS_MERGE = String.format("BEGIN%n" +
                "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:0}), (n2:Bar{name:\"bar\"}) MERGE (n1)-[r:KNOWS]->(n2) SET r.since=2016;%n" +
                "COMMIT%n");

        public static final String EXPECTED_RELATIONSHIPS_MERGE_ON_CREATE_SET =
                EXPECTED_RELATIONSHIPS_MERGE.replaceAll(" SET ", " ON CREATE SET ");

        public static final String EXPECTED_CLEAN_UP = String.format("BEGIN%n" +
                "MATCH (n:`UNIQUE IMPORT LABEL`)  WITH n LIMIT 20000 REMOVE n:`UNIQUE IMPORT LABEL` REMOVE n.`UNIQUE IMPORT ID`;%n" +
                "COMMIT%n" +
                "BEGIN%n" +
                "DROP CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT (node.`UNIQUE IMPORT ID`) IS UNIQUE;%n" +
                "COMMIT%n");

        public static final String EXPECTED_CLEAN_UP_EMPTY = String.format("BEGIN%n" +
                "COMMIT%n" +
                "BEGIN%n" +
                "COMMIT%n");

        public static final String EXPECTED_ONLY_SCHEMA_NEO4J_SHELL = String.format("BEGIN%n" +
                "CREATE BTREE INDEX FOR (node:Bar) ON (node.first_name, node.last_name);%n" +
                "CREATE BTREE INDEX FOR (node:Foo) ON (node.name);%n" +
                "CREATE CONSTRAINT ON (node:Bar) ASSERT (node.name) IS UNIQUE;%n" +
                "CREATE CONSTRAINT ON (node:Bar) ASSERT (node.name, node.age) IS UNIQUE;%n" +
                "COMMIT%n" +
                "SCHEMA AWAIT%n");

        static final String EXPECTED_ONLY_SCHEMA_NEO4J_SHELL_WITH_NAMES = "BEGIN\n" +
                "CREATE BTREE INDEX barIndex FOR (node:Bar) ON (node.first_name, node.last_name);\n" +
                "CREATE BTREE INDEX fooIndex FOR (node:Foo) ON (node.name);\n" +
                "CREATE CONSTRAINT consBar ON (node:Bar) ASSERT (node.name) IS UNIQUE;\n" +
                "CREATE CONSTRAINT uniqueConstraintComposite ON (node:Bar) ASSERT (node.name, node.age) IS UNIQUE;\n" +
                "COMMIT\n" +
                "SCHEMA AWAIT\n";

        public static final String EXPECTED_CYPHER_POINT = String.format("BEGIN%n" +
                "CREATE (:Test:`UNIQUE IMPORT LABEL` {name:\"foo\", place2d:point({x: 2.3, y: 4.5, crs: 'cartesian'}), place3d1:point({x: 2.3, y: 4.5, z: 1.2, crs: 'cartesian-3d'}), `UNIQUE IMPORT ID`:3});%n" +
                "CREATE (:Bar:`UNIQUE IMPORT LABEL` {place3d:point({x: 12.78, y: 56.7, z: 100.0, crs: 'wgs-84-3d'}), `UNIQUE IMPORT ID`:4});%n" +
                "COMMIT%n" +
                "BEGIN%n" +
                "CREATE BTREE INDEX FOR (node:Bar) ON (node.first_name, node.last_name);%n" +
                "CREATE CONSTRAINT ON (node:Bar) ASSERT (node.name) IS UNIQUE;%n" +
                "CREATE CONSTRAINT ON (node:Bar) ASSERT (node.name, node.age) IS UNIQUE;%n" +
                "CREATE CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT (node.`UNIQUE IMPORT ID`) IS UNIQUE;%n" +
                "COMMIT%n" +
                "SCHEMA AWAIT%n" +
                "BEGIN%n" +
                "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:3}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:4}) CREATE (n1)-[r:FRIEND_OF {place2d:point({x: 56.7, y: 12.78, crs: 'wgs-84'})}]->(n2);%n" +
                "COMMIT%n" +
                "BEGIN%n" +
                "MATCH (n:`UNIQUE IMPORT LABEL`)  WITH n LIMIT 20000 REMOVE n:`UNIQUE IMPORT LABEL` REMOVE n.`UNIQUE IMPORT ID`;%n" +
                "COMMIT%n" +
                "BEGIN%n" +
                "DROP CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT (node.`UNIQUE IMPORT ID`) IS UNIQUE;%n" +
                "COMMIT%n");

        public static final String EXPECTED_CYPHER_DATE = String.format("BEGIN%n" +
                "CREATE (:Test:`UNIQUE IMPORT LABEL` {date:date('2018-10-30'), datetime:datetime('2018-10-30T12:50:35.556+01:00'), localTime:localdatetime('2018-10-30T19:32:24'), name:\"foo\", `UNIQUE IMPORT ID`:3});%n" +
                "CREATE (:Bar:`UNIQUE IMPORT LABEL` {datetime:datetime('2018-10-30T12:50:35.556Z'), `UNIQUE IMPORT ID`:4});%n" +
                "COMMIT%n" +
                "BEGIN%n" +
                "CREATE BTREE INDEX FOR (node:Bar) ON (node.first_name, node.last_name);%n" +
                "CREATE CONSTRAINT ON (node:Bar) ASSERT (node.name) IS UNIQUE;%n" +
                "CREATE CONSTRAINT ON (node:Bar) ASSERT (node.name, node.age) IS UNIQUE;%n" +
                "CREATE CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT (node.`UNIQUE IMPORT ID`) IS UNIQUE;%n" +
                "COMMIT%n" +
                "SCHEMA AWAIT%n" +
                "BEGIN%n" +
                "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:3}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:4}) CREATE (n1)-[r:FRIEND_OF {date:date('2018-10-30')}]->(n2);%n" +
                "COMMIT%n" +
                "BEGIN%n" +
                "MATCH (n:`UNIQUE IMPORT LABEL`)  WITH n LIMIT 20000 REMOVE n:`UNIQUE IMPORT LABEL` REMOVE n.`UNIQUE IMPORT ID`;%n" +
                "COMMIT%n" +
                "BEGIN%n" +
                "DROP CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT (node.`UNIQUE IMPORT ID`) IS UNIQUE;%n" +
                "COMMIT%n");

        public static final String EXPECTED_CYPHER_TIME = String.format("BEGIN%n" +
                "CREATE (:Test:`UNIQUE IMPORT LABEL` {local:localtime('12:50:35.556'), name:\"foo\", t:time('12:50:35.556+01:00'), `UNIQUE IMPORT ID`:3});%n" +
                "CREATE (:Bar:`UNIQUE IMPORT LABEL` {datetime:datetime('2018-10-30T12:50:35.556+01:00'), `UNIQUE IMPORT ID`:4});%n" +
                "COMMIT%n" +
                "BEGIN%n" +
                "CREATE BTREE INDEX FOR (node:Bar) ON (node.first_name, node.last_name);%n" +
                "CREATE CONSTRAINT ON (node:Bar) ASSERT (node.name) IS UNIQUE;%n" +
                "CREATE CONSTRAINT ON (node:Bar) ASSERT (node.name, node.age) IS UNIQUE;%n" +
                "CREATE CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT (node.`UNIQUE IMPORT ID`) IS UNIQUE;%n" +
                "COMMIT%n" +
                "SCHEMA AWAIT%n" +
                "BEGIN%n" +
                "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:3}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:4}) CREATE (n1)-[r:FRIEND_OF {t:time('12:50:35.556+01:00')}]->(n2);%n" +
                "COMMIT%n" +
                "BEGIN%n" +
                "MATCH (n:`UNIQUE IMPORT LABEL`)  WITH n LIMIT 20000 REMOVE n:`UNIQUE IMPORT LABEL` REMOVE n.`UNIQUE IMPORT ID`;%n" +
                "COMMIT%n" +
                "BEGIN%n" +
                "DROP CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT (node.`UNIQUE IMPORT ID`) IS UNIQUE;%n" +
                "COMMIT%n");

        public static final String EXPECTED_CYPHER_DURATION = String.format("BEGIN%n" +
                "CREATE (:Test:`UNIQUE IMPORT LABEL` {duration:duration('P5M1DT12H'), name:\"foo\", `UNIQUE IMPORT ID`:3});%n" +
                "CREATE (:Bar:`UNIQUE IMPORT LABEL` {duration:duration('P5M1DT12H'), `UNIQUE IMPORT ID`:4});%n" +
                "COMMIT%n" +
                "BEGIN%n" +
                "CREATE BTREE INDEX FOR (node:Bar) ON (node.first_name, node.last_name);%n" +
                "CREATE CONSTRAINT ON (node:Bar) ASSERT (node.name) IS UNIQUE;%n" +
                "CREATE CONSTRAINT ON (node:Bar) ASSERT (node.name, node.age) IS UNIQUE;%n" +
                "CREATE CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT (node.`UNIQUE IMPORT ID`) IS UNIQUE;%n" +
                "COMMIT%n" +
                "SCHEMA AWAIT%n" +
                "BEGIN%n" +
                "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:3}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:4}) CREATE (n1)-[r:FRIEND_OF {duration:duration('P5M1DT12H')}]->(n2);%n" +
                "COMMIT%n" +
                "BEGIN%n" +
                "MATCH (n:`UNIQUE IMPORT LABEL`)  WITH n LIMIT 20000 REMOVE n:`UNIQUE IMPORT LABEL` REMOVE n.`UNIQUE IMPORT ID`;%n" +
                "COMMIT%n" +
                "BEGIN%n" +
                "DROP CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT (node.`UNIQUE IMPORT ID`) IS UNIQUE;%n" +
                "COMMIT%n");

        public static final String EXPECTED_CYPHER_LABELS_ASCENDEND = String.format("BEGIN%n" +
                "CREATE (:User:User0:User1:User12:`UNIQUE IMPORT LABEL` {name:\"Alan\", `UNIQUE IMPORT ID`:3});%n" +
                "COMMIT%n" +
                "BEGIN%n" +
                "CREATE CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT (node.`UNIQUE IMPORT ID`) IS UNIQUE;%n" +
                "COMMIT%n" +
                "SCHEMA AWAIT%n" +
                "BEGIN%n" +
                "MATCH (n:`UNIQUE IMPORT LABEL`)  WITH n LIMIT 20000 REMOVE n:`UNIQUE IMPORT LABEL` REMOVE n.`UNIQUE IMPORT ID`;%n" +
                "COMMIT%n" +
                "BEGIN%n" +
                "DROP CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT (node.`UNIQUE IMPORT ID`) IS UNIQUE;%n" +
                "COMMIT%n");

        static final String EXPECTED_SCHEMA_WITH_RELS_OPTIMIZED = String.format("BEGIN%n" +
                "CREATE BTREE INDEX FOR (node:Bar) ON (node.first_name, node.last_name);%n" +
                "CREATE BTREE INDEX FOR (node:Foo) ON (node.name);%n" +
                "CREATE BTREE INDEX FOR ()-[rel:KNOWS]-() ON (rel.since, rel.foo);%n" +
                "CREATE CONSTRAINT ON (node:Bar) ASSERT (node.name) IS UNIQUE;%n" +
                "CREATE CONSTRAINT ON (node:Bar) ASSERT (node.name, node.age) IS UNIQUE;%n" +
                "CREATE CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT (node.`UNIQUE IMPORT ID`) IS UNIQUE;%n" +
                "COMMIT%n" +
                "SCHEMA AWAIT%n");

        static final String EXPECTED_SCHEMA_WITH_RELS_AND_NAME_OPTIMIZED = String.format("BEGIN%n" +
                "CREATE BTREE INDEX barIndex FOR (node:Bar) ON (node.first_name, node.last_name);%n" +
                "CREATE BTREE INDEX fooIndex FOR (node:Foo) ON (node.name);%n" +
                "CREATE BTREE INDEX rel_index_name FOR ()-[rel:KNOWS]-() ON (rel.since, rel.foo);%n" +
                "CREATE CONSTRAINT consBar ON (node:Bar) ASSERT (node.name) IS UNIQUE;%n" +
                "CREATE CONSTRAINT uniqueConstraintComposite ON (node:Bar) ASSERT (node.name, node.age) IS UNIQUE;%n" +
                "CREATE CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT (node.`UNIQUE IMPORT ID`) IS UNIQUE;%n" +
                "COMMIT%n" +
                "SCHEMA AWAIT%n");

        static final String EXPECTED_SCHEMA_WITH_RELS_AND_IF_NOT_EXISTS = String.format("BEGIN%n" +
                "CREATE BTREE INDEX IF NOT EXISTS FOR (node:Bar) ON (node.first_name, node.last_name);%n" +
                "CREATE BTREE INDEX IF NOT EXISTS FOR (node:Foo) ON (node.name);%n" +
                "CREATE BTREE INDEX IF NOT EXISTS FOR ()-[rel:KNOWS]-() ON (rel.since, rel.foo);%n" +
                "CREATE CONSTRAINT IF NOT EXISTS ON (node:Bar) ASSERT (node.name) IS UNIQUE;%n" +
                "CREATE CONSTRAINT IF NOT EXISTS ON (node:Bar) ASSERT (node.name, node.age) IS UNIQUE;%n" +
                "CREATE CONSTRAINT IF NOT EXISTS ON (node:`UNIQUE IMPORT LABEL`) ASSERT (node.`UNIQUE IMPORT ID`) IS UNIQUE;%n" +
                "COMMIT%n" +
                "SCHEMA AWAIT%n");

        static final String EXPECTED_SCHEMA_OPTIMIZED = String.format("BEGIN%n" +
                "CREATE BTREE INDEX FOR (node:Bar) ON (node.first_name, node.last_name);%n" +
                "CREATE BTREE INDEX FOR (node:Foo) ON (node.name);%n" +
                "CREATE CONSTRAINT ON (node:Bar) ASSERT (node.name) IS UNIQUE;%n" +
                "CREATE CONSTRAINT ON (node:Bar) ASSERT (node.name, node.age) IS UNIQUE;%n" +
                "CREATE CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT (node.`UNIQUE IMPORT ID`) IS UNIQUE;%n" +
                "COMMIT%n" +
                "SCHEMA AWAIT%n");

        static final String EXPECTED_SCHEMA_OPTIMIZED_WITH_IF_NOT_EXISTS = String.format("BEGIN%n" +
                "CREATE BTREE INDEX IF NOT EXISTS FOR (node:Bar) ON (node.first_name, node.last_name);%n" +
                "CREATE BTREE INDEX IF NOT EXISTS FOR (node:Foo) ON (node.name);%n" +
                "CREATE CONSTRAINT IF NOT EXISTS ON (node:Bar) ASSERT (node.name) IS UNIQUE;%n" +
                "CREATE CONSTRAINT IF NOT EXISTS ON (node:Bar) ASSERT (node.name, node.age) IS UNIQUE;%n" +
                "CREATE CONSTRAINT IF NOT EXISTS ON (node:`UNIQUE IMPORT LABEL`) ASSERT (node.`UNIQUE IMPORT ID`) IS UNIQUE;%n" +
                "COMMIT%n" +
                "SCHEMA AWAIT%n");

        static final String EXPECTED_SCHEMA_OPTIMIZED_WITH_RELS_IF_NOT_EXISTS_AND_NAME = String.format("BEGIN%n" +
                "CREATE BTREE INDEX barIndex IF NOT EXISTS FOR (node:Bar) ON (node.first_name, node.last_name);%n" +
                "CREATE BTREE INDEX fooIndex IF NOT EXISTS FOR (node:Foo) ON (node.name);%n" +
                "CREATE BTREE INDEX rel_index_name IF NOT EXISTS FOR ()-[rel:KNOWS]-() ON (rel.since, rel.foo);%n" +
                "CREATE CONSTRAINT consBar IF NOT EXISTS ON (node:Bar) ASSERT (node.name) IS UNIQUE;%n" +
                "CREATE CONSTRAINT uniqueConstraintComposite IF NOT EXISTS ON (node:Bar) ASSERT (node.name, node.age) IS UNIQUE;%n" +
                "CREATE CONSTRAINT IF NOT EXISTS ON (node:`UNIQUE IMPORT LABEL`) ASSERT (node.`UNIQUE IMPORT ID`) IS UNIQUE;%n" +
                "COMMIT%n" +
                "SCHEMA AWAIT%n");

        static final String EXPECTED_NODES_OPTIMIZED_BATCH_SIZE = String.format("BEGIN%n" +
                "UNWIND [{_id:3, properties:{age:17}}] AS row%n" +
                "CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Bar;%n" +
                "UNWIND [{_id:2, properties:{age:12}}] AS row%n" +
                "CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Bar:Person;%n" +
                "UNWIND [{_id:0, properties:{born:date('2018-10-31'), name:\"foo\"}}, {_id:4, properties:{born:date('2017-09-29'), name:\"foo2\"}}] AS row%n" +
                "CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Foo;%n" +
                "UNWIND [{name:\"bar\", properties:{age:42}}, {name:\"bar2\", properties:{age:44}}] AS row%n" +
                "CREATE (n:Bar{name: row.name}) SET n += row.properties;%n" +
                "UNWIND [{_id:6, properties:{age:99}}] AS row%n" +
                "CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties;%n" +
                "COMMIT%n");

        public static final String EXPECTED_NODES_OPTIMIZED = String.format("BEGIN%n" +
                "UNWIND [{_id:3, properties:{age:17}}] AS row%n" +
                "CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Bar;%n" +
                "UNWIND [{_id:2, properties:{age:12}}] AS row%n" +
                "CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Bar:Person;%n" +
                "UNWIND [{_id:0, properties:{born:date('2018-10-31'), name:\"foo\"}}, {_id:4, properties:{born:date('2017-09-29'), name:\"foo2\"}}] AS row%n" +
                "CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Foo;%n" +
                "UNWIND [{name:\"bar\", properties:{age:42}}, {name:\"bar2\", properties:{age:44}}] AS row%n" +
                "CREATE (n:Bar{name: row.name}) SET n += row.properties;%n" +
                "UNWIND [{_id:6, properties:{age:99}}] AS row%n" +
                "CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties;%n" +
                "COMMIT%n");

        static final String EXPECTED_QUERY_NODES_OPTIMIZED = String.format("BEGIN%n" +
                "UNWIND [{_id:0, properties:{born:date('2018-10-31'), name:\"foo\"}}, {_id:4, properties:{born:date('2017-09-29'), name:\"foo2\"}}] AS row%n" +
                "CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Foo;%n" +
                "UNWIND [{name:\"bar\", properties:{age:42}}, {name:\"bar2\", properties:{age:44}}] AS row%n" +
                "CREATE (n:Bar{name: row.name}) SET n += row.properties;%n" +
                "COMMIT%n");

        static final String EXPECTED_QUERY_NODES_OPTIMIZED2 = String.format("BEGIN%n" +
                "UNWIND [{_id:4, properties:{born:date('2017-09-29'), name:\"foo2\"}}, {_id:0, properties:{born:date('2018-10-31'), name:\"foo\"}}] AS row%n" +
                "CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Foo;%n" +
                "UNWIND [{name:\"bar\", properties:{age:42}}, {name:\"bar2\", properties:{age:44}}] AS row%n" +
                "CREATE (n:Bar{name: row.name}) SET n += row.properties;%n" +
                "COMMIT%n");

        public static final String EXPECTED_RELATIONSHIPS_OPTIMIZED = String.format("BEGIN%n" +
                "UNWIND [{start: {_id:0}, end: {name:\"bar\"}, properties:{since:2016}}, {start: {_id:4}, end: {name:\"bar2\"}, properties:{since:2015}}] AS row%n" +
                "MATCH (start:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row.start._id})%n" +
                "MATCH (end:Bar{name: row.end.name})%n" +
                "CREATE (start)-[r:KNOWS]->(end) SET r += row.properties;%n" +
                "COMMIT%n");

        static final String EXPECTED_RELATIONSHIPS_ODD = String.format("BEGIN%n" +
                "UNWIND [{start: {_id:0}, end: {name:\"bar\"}, properties:{since:2016}}] AS row%n" +
                "MATCH (start:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row.start._id})%n" +
                "MATCH (end:Bar{name: row.end.name})%n" +
                "CREATE (start)-[r:KNOWS]->(end) SET r += row.properties;%n" +
                "COMMIT%n");

        static final String EXPECTED_RELATIONSHIPS_PARAMS_ODD = String.format(
                "BEGIN%n" +
                ":param rows => [{start: {_id:0}, end: {name:\"bar\"}, properties:{since:2016}}]%n" +
                "UNWIND $rows AS row%n" +
                "MATCH (start:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row.start._id})%n" +
                "MATCH (end:Bar{name: row.end.name})%n" +
                "CREATE (start)-[r:KNOWS]->(end) SET r += row.properties;%n" +
                "COMMIT%n");

        static final String DROP_UNIQUE_OPTIMIZED = String.format("BEGIN%n" +
                "MATCH (n:`UNIQUE IMPORT LABEL`)  WITH n LIMIT 20000 REMOVE n:`UNIQUE IMPORT LABEL` REMOVE n.`UNIQUE IMPORT ID`;%n" +
                "COMMIT%n" +
                "BEGIN%n" +
                "DROP CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT (node.`UNIQUE IMPORT ID`) IS UNIQUE;%n" +
                "COMMIT%n");

        static final String DROP_UNIQUE_OPTIMIZED_BATCH = String.format("BEGIN%n" +
                "MATCH (n:`UNIQUE IMPORT LABEL`)  WITH n LIMIT 2 REMOVE n:`UNIQUE IMPORT LABEL` REMOVE n.`UNIQUE IMPORT ID`;%n" +
                "COMMIT%n" +
                "BEGIN%n" +
                "MATCH (n:`UNIQUE IMPORT LABEL`)  WITH n LIMIT 2 REMOVE n:`UNIQUE IMPORT LABEL` REMOVE n.`UNIQUE IMPORT ID`;%n" +
                "COMMIT%n" +
                "BEGIN%n" +
                "MATCH (n:`UNIQUE IMPORT LABEL`)  WITH n LIMIT 2 REMOVE n:`UNIQUE IMPORT LABEL` REMOVE n.`UNIQUE IMPORT ID`;%n" +
                "COMMIT%n" +
                "BEGIN%n" +
                "DROP CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT (node.`UNIQUE IMPORT ID`) IS UNIQUE;%n" +
                "COMMIT%n");

        static final String EXPECTED_NODES_OPTIMIZED_BATCH_SIZE_UNWIND = String.format("BEGIN%n" +
                "UNWIND [{_id:3, properties:{age:17}}] AS row%n" +
                "CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Bar;%n" +
                "UNWIND [{_id:2, properties:{age:12}}] AS row%n" +
                "CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Bar:Person;%n" +
                "COMMIT%n" +
                "BEGIN%n" +
                "UNWIND [{_id:0, properties:{born:date('2018-10-31'), name:\"foo\"}}, {_id:4, properties:{born:date('2017-09-29'), name:\"foo2\"}}] AS row%n" +
                "CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Foo;%n" +
                "COMMIT%n" +
                "BEGIN%n" +
                "UNWIND [{name:\"bar\", properties:{age:42}}, {name:\"bar2\", properties:{age:44}}] AS row%n" +
                "CREATE (n:Bar{name: row.name}) SET n += row.properties;%n" +
                "COMMIT%n" +
                "BEGIN%n" +
                "UNWIND [{_id:6, properties:{age:99}}] AS row%n" +
                "CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties;%n" +
                "COMMIT%n");

        static final String EXPECTED_NODES_OPTIMIZED_BATCH_SIZE_ODD = String.format("BEGIN%n" +
                "UNWIND [{_id:4, properties:{age:12}}, {_id:5, properties:{age:4}}] AS row%n" +
                "CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Bar;%n" +
                "COMMIT%n" +
                "BEGIN%n" +
                "UNWIND [{_id:0, properties:{born:date('2018-10-31'), name:\"foo\"}}, {_id:1, properties:{born:date('2017-09-29'), name:\"foo2\"}}] AS row%n" +
                "CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Foo;%n" +
                "COMMIT%n" +
                "BEGIN%n" +
                "UNWIND [{_id:2, properties:{born:date('2016-03-12'), name:\"foo3\"}}] AS row%n" +
                "CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Foo;%n" +
                "UNWIND [{name:\"bar\", properties:{age:42}}] AS row%n" +
                "CREATE (n:Bar{name: row.name}) SET n += row.properties;%n" +
                "COMMIT%n" +
                "BEGIN%n" +
                "UNWIND [{name:\"bar2\", properties:{age:44}}] AS row%n" +
                "CREATE (n:Bar{name: row.name}) SET n += row.properties;%n" +
                "COMMIT%n");

        static final String EXPECTED_NODES_OPTIMIZED_PARAMS_BATCH_SIZE_ODD = String.format(
                ":begin%n" +
                ":param rows => [{_id:4, properties:{age:12}}, {_id:5, properties:{age:4}}]%n" +
                "UNWIND $rows AS row%n" +
                "CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Bar;%n" +
                ":commit%n" +
                ":begin%n" +
                ":param rows => [{_id:0, properties:{born:date('2018-10-31'), name:\"foo\"}}, {_id:1, properties:{born:date('2017-09-29'), name:\"foo2\"}}]%n" +
                "UNWIND $rows AS row%n" +
                "CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Foo;%n" +
                ":commit%n" +
                ":begin%n" +
                ":param rows => [{_id:2, properties:{born:date('2016-03-12'), name:\"foo3\"}}]%n" +
                "UNWIND $rows AS row%n" +
                "CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Foo;%n" +
                ":param rows => [{name:\"bar\", properties:{age:42}}]%n" +
                "UNWIND $rows AS row%n" +
                "CREATE (n:Bar{name: row.name}) SET n += row.properties;%n" +
                ":commit%n" +
                ":begin%n" +
                ":param rows => [{name:\"bar2\", properties:{age:44}}]%n" +
                "UNWIND $rows AS row%n" +
                "CREATE (n:Bar{name: row.name}) SET n += row.properties;%n" +
                ":commit%n");

        public static final String EXPECTED_PLAIN_ADD_STRUCTURE_UNWIND = String.format("UNWIND [{_id:3, properties:{age:17}}] AS row%n" +
                "MERGE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) ON CREATE SET n += row.properties SET n:Bar;%n" +
                "UNWIND [{_id:2, properties:{age:12}}] AS row%n" +
                "MERGE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) ON CREATE SET n += row.properties SET n:Bar:Person;%n" +
                "UNWIND [{_id:0, properties:{born:date('2018-10-31'), name:\"foo\"}}, {_id:4, properties:{born:date('2017-09-29'), name:\"foo2\"}}] AS row%n" +
                "MERGE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) ON CREATE SET n += row.properties SET n:Foo;%n" +
                "UNWIND [{name:\"bar\", properties:{age:42}}, {name:\"bar2\", properties:{age:44}}] AS row%n" +
                "MERGE (n:Bar{name: row.name}) ON CREATE SET n += row.properties;%n" +
                "UNWIND [{_id:6, properties:{age:99}}] AS row%n" +
                "MERGE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) ON CREATE SET n += row.properties;%n" +
                "UNWIND [{start: {_id:0}, end: {name:\"bar\"}, properties:{since:2016}}, {start: {_id:4}, end: {name:\"bar2\"}, properties:{since:2015}}] AS row%n" +
                "MATCH (start:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row.start._id})%n" +
                "MATCH (end:Bar{name: row.end.name})%n" +
                "CREATE (start)-[r:KNOWS]->(end)  SET r += row.properties;%n");

        public static final String EXPECTED_UPDATE_ALL_UNWIND = String.format("CREATE BTREE INDEX FOR (node:Bar) ON (node.first_name, node.last_name);%n" +
                "CREATE BTREE INDEX FOR (node:Foo) ON (node.name);%n" +
                "CREATE CONSTRAINT ON (node:Bar) ASSERT (node.name) IS UNIQUE;%n" +
                "CREATE CONSTRAINT ON (node:Bar) ASSERT (node.name, node.age) IS UNIQUE;%n" +
                "CREATE CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT (node.`UNIQUE IMPORT ID`) IS UNIQUE;%n" +
                "UNWIND [{_id:3, properties:{age:17}}] AS row%n" +
                "MERGE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Bar;%n" +
                "UNWIND [{_id:2, properties:{age:12}}] AS row%n" +
                "MERGE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Bar:Person;%n" +
                "UNWIND [{_id:0, properties:{born:date('2018-10-31'), name:\"foo\"}}, {_id:4, properties:{born:date('2017-09-29'), name:\"foo2\"}}] AS row%n" +
                "MERGE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Foo;%n" +
                "UNWIND [{name:\"bar\", properties:{age:42}}, {name:\"bar2\", properties:{age:44}}] AS row%n" +
                "MERGE (n:Bar{name: row.name}) SET n += row.properties;%n" +
                "UNWIND [{_id:6, properties:{age:99}}] AS row%n" +
                "MERGE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties;%n" +
                "UNWIND [{start: {_id:0}, end: {name:\"bar\"}, properties:{since:2016}}, {start: {_id:4}, end: {name:\"bar2\"}, properties:{since:2015}}] AS row%n" +
                "MATCH (start:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row.start._id})%n" +
                "MATCH (end:Bar{name: row.end.name})%n" +
                "MERGE (start)-[r:KNOWS]->(end) SET r += row.properties;%n" +
                "MATCH (n:`UNIQUE IMPORT LABEL`)  WITH n LIMIT 20000 REMOVE n:`UNIQUE IMPORT LABEL` REMOVE n.`UNIQUE IMPORT ID`;%n" +
                "DROP CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT (node.`UNIQUE IMPORT ID`) IS UNIQUE;%n");

        public static final String EXPECTED_PLAIN_UPDATE_STRUCTURE_UNWIND = String.format("UNWIND [{start: {_id:0}, end: {name:\"bar\"}, properties:{since:2016}}, {start: {_id:4}, end: {name:\"bar2\"}, properties:{since:2015}}] AS row%n" +
                "MATCH (start:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row.start._id})%n" +
                "MATCH (end:Bar{name: row.end.name})%n" +
                "MERGE (start)-[r:KNOWS]->(end) SET r += row.properties;%n");

        public static final String EXPECTED_NEO4J_OPTIMIZED = EXPECTED_SCHEMA_OPTIMIZED + EXPECTED_NODES_OPTIMIZED + EXPECTED_RELATIONSHIPS_OPTIMIZED + DROP_UNIQUE_OPTIMIZED;

        public static final String EXPECTED_NEO4J_OPTIMIZED_BATCH_SIZE = EXPECTED_SCHEMA_OPTIMIZED + EXPECTED_NODES_OPTIMIZED_BATCH_SIZE + EXPECTED_RELATIONSHIPS_OPTIMIZED + DROP_UNIQUE_OPTIMIZED;

        static final String EXPECTED_NEO4J_SHELL_OPTIMIZED = EXPECTED_SCHEMA_OPTIMIZED + EXPECTED_NODES_OPTIMIZED + EXPECTED_RELATIONSHIPS_OPTIMIZED + DROP_UNIQUE_OPTIMIZED;

        static final String EXPECTED_NEO4J_SHELL_OPTIMIZED_BATCH_SIZE = EXPECTED_SCHEMA_OPTIMIZED + EXPECTED_NODES_OPTIMIZED_BATCH_SIZE + EXPECTED_RELATIONSHIPS_OPTIMIZED + DROP_UNIQUE_OPTIMIZED;

        static final String EXPECTED_QUERY_NODES =  EXPECTED_SCHEMA_OPTIMIZED + EXPECTED_QUERY_NODES_OPTIMIZED + EXPECTED_RELATIONSHIPS_OPTIMIZED + DROP_UNIQUE_OPTIMIZED;
        static final String EXPECTED_QUERY_NODES2 =  EXPECTED_SCHEMA_OPTIMIZED + EXPECTED_QUERY_NODES_OPTIMIZED2 + EXPECTED_RELATIONSHIPS_OPTIMIZED + DROP_UNIQUE_OPTIMIZED;

        static final String EXPECTED_CYPHER_OPTIMIZED_BATCH_SIZE_UNWIND = EXPECTED_SCHEMA_OPTIMIZED + EXPECTED_NODES_OPTIMIZED_BATCH_SIZE_UNWIND + EXPECTED_RELATIONSHIPS_OPTIMIZED + DROP_UNIQUE_OPTIMIZED_BATCH;

        static final String EXPECTED_CYPHER_OPTIMIZED_BATCH_SIZE_ODD = EXPECTED_SCHEMA_OPTIMIZED + EXPECTED_NODES_OPTIMIZED_BATCH_SIZE_ODD + EXPECTED_RELATIONSHIPS_ODD + DROP_UNIQUE_OPTIMIZED_BATCH;

        static final String EXPECTED_CYPHER_SHELL_PARAMS_OPTIMIZED_ODD = EXPECTED_SCHEMA_OPTIMIZED + EXPECTED_NODES_OPTIMIZED_PARAMS_BATCH_SIZE_ODD + EXPECTED_RELATIONSHIPS_PARAMS_ODD + DROP_UNIQUE_OPTIMIZED_BATCH;


        public static final String EXPECTED_QUERY_CYPHER_SHELL_OPTIMIZED_UNWIND = EXPECTED_CYPHER_OPTIMIZED_BATCH_SIZE_UNWIND
                .replace(NEO4J_SHELL.begin(), CYPHER_SHELL.begin())
                .replace(NEO4J_SHELL.commit(), CYPHER_SHELL.commit())
                .replace(NEO4J_SHELL.schemaAwait(), EXPECTED_INDEXES_AWAIT)
                .replace(NEO4J_SHELL.schemaAwait(), CYPHER_SHELL.schemaAwait());


        public static final String EXPECTED_QUERY_CYPHER_SHELL_OPTIMIZED_ODD = EXPECTED_CYPHER_OPTIMIZED_BATCH_SIZE_ODD
                .replace(NEO4J_SHELL.begin(), CYPHER_SHELL.begin())
                .replace(NEO4J_SHELL.commit(), CYPHER_SHELL.commit())
                .replace(NEO4J_SHELL.schemaAwait(), EXPECTED_INDEXES_AWAIT)
                .replace(NEO4J_SHELL.schemaAwait(), CYPHER_SHELL.schemaAwait());

        public static final String EXPECTED_QUERY_CYPHER_SHELL_PARAMS_OPTIMIZED_ODD = EXPECTED_CYPHER_SHELL_PARAMS_OPTIMIZED_ODD
                .replace(NEO4J_SHELL.begin(), CYPHER_SHELL.begin())
                .replace(NEO4J_SHELL.commit(), CYPHER_SHELL.commit())
                .replace(NEO4J_SHELL.schemaAwait(), EXPECTED_INDEXES_AWAIT)
                .replace(NEO4J_SHELL.schemaAwait(), CYPHER_SHELL.schemaAwait());

        static final String EXPECTED_QUERY_CYPHER_SHELL_OPTIMIZED = EXPECTED_QUERY_NODES
                .replace(NEO4J_SHELL.begin(), CYPHER_SHELL.begin())
                .replace(NEO4J_SHELL.commit(), CYPHER_SHELL.commit())
                .replace(NEO4J_SHELL.schemaAwait(), EXPECTED_INDEXES_AWAIT_QUERY)
                .replace(NEO4J_SHELL.schemaAwait(), CYPHER_SHELL.schemaAwait());

        static final String EXPECTED_QUERY_CYPHER_SHELL_OPTIMIZED2 = EXPECTED_QUERY_NODES2
                .replace(NEO4J_SHELL.begin(), CYPHER_SHELL.begin())
                .replace(NEO4J_SHELL.commit(), CYPHER_SHELL.commit())
                .replace(NEO4J_SHELL.schemaAwait(), EXPECTED_INDEXES_AWAIT_QUERY)
                .replace(NEO4J_SHELL.schemaAwait(), CYPHER_SHELL.schemaAwait());

        public static final String EXPECTED_CYPHER_SHELL_OPTIMIZED = EXPECTED_NEO4J_SHELL_OPTIMIZED
                .replace(NEO4J_SHELL.begin(), CYPHER_SHELL.begin())
                .replace(NEO4J_SHELL.commit(), CYPHER_SHELL.commit())
                .replace(NEO4J_SHELL.schemaAwait(), EXPECTED_INDEXES_AWAIT)
                .replace(NEO4J_SHELL.schemaAwait(), CYPHER_SHELL.schemaAwait());

        public static final String EXPECTED_CYPHER_SHELL_OPTIMIZED_BATCH_SIZE = EXPECTED_NEO4J_SHELL_OPTIMIZED_BATCH_SIZE
                .replace(NEO4J_SHELL.begin(), CYPHER_SHELL.begin())
                .replace(NEO4J_SHELL.commit(), CYPHER_SHELL.commit())
                .replace(NEO4J_SHELL.schemaAwait(), EXPECTED_INDEXES_AWAIT)
                .replace(NEO4J_SHELL.schemaAwait(), CYPHER_SHELL.schemaAwait());

        public static final String EXPECTED_PLAIN_OPTIMIZED_BATCH_SIZE = EXPECTED_NEO4J_SHELL_OPTIMIZED_BATCH_SIZE
                .replace(NEO4J_SHELL.begin(), PLAIN_FORMAT.begin())
                .replace(NEO4J_SHELL.commit(), PLAIN_FORMAT.commit())
                .replace(NEO4J_SHELL.schemaAwait(), PLAIN_FORMAT.schemaAwait());

        public static final String EXPECTED_NEO4J_SHELL = EXPECTED_NODES + EXPECTED_SCHEMA + EXPECTED_RELATIONSHIPS + EXPECTED_CLEAN_UP;

        public static final String EXPECTED_CYPHER_SHELL = EXPECTED_NEO4J_SHELL
                .replace(NEO4J_SHELL.begin(), CYPHER_SHELL.begin())
                .replace(NEO4J_SHELL.commit(), CYPHER_SHELL.commit())
                .replace(NEO4J_SHELL.schemaAwait(), EXPECTED_INDEXES_AWAIT)
                .replace(NEO4J_SHELL.schemaAwait(), CYPHER_SHELL.schemaAwait());

        public static final String EXPECTED_PLAIN = EXPECTED_NEO4J_SHELL
                .replace(NEO4J_SHELL.begin(), PLAIN_FORMAT.begin()).replace(NEO4J_SHELL.commit(), PLAIN_FORMAT.commit())
                .replace(NEO4J_SHELL.schemaAwait(), PLAIN_FORMAT.schemaAwait());

        public static final String EXPECTED_NEO4J_MERGE = EXPECTED_NODES_MERGE + EXPECTED_SCHEMA + EXPECTED_RELATIONSHIPS_MERGE + EXPECTED_CLEAN_UP;

        public static final String EXPECTED_ONLY_SCHEMA_CYPHER_SHELL = EXPECTED_ONLY_SCHEMA_NEO4J_SHELL.replace(NEO4J_SHELL.begin(), CYPHER_SHELL.begin())
                .replace(NEO4J_SHELL.commit(), CYPHER_SHELL.commit()).replace(NEO4J_SHELL.schemaAwait(), CYPHER_SHELL.schemaAwait()) + EXPECTED_INDEXES_AWAIT;


        static final String EXPECTED_NODES_COMPOUND_CONSTRAINT = String.format("BEGIN%n" +
                "CREATE (:`Person` {`name`:\"John\", `surname`:\"Snow\"});%n" +
                "CREATE (:`Person` {`name`:\"Matt\", `surname`:\"Jackson\"});%n" +
                "CREATE (:`Person` {`name`:\"Jenny\", `surname`:\"White\"});%n" +
                "CREATE (:`Person` {`name`:\"Susan\", `surname`:\"Brown\"});%n" +
                "CREATE (:`Person` {`name`:\"Tom\", `surname`:\"Taylor\"});%n" +
                "COMMIT%n");

        static final String EXPECTED_SCHEMA_COMPOUND_CONSTRAINT = String.format("BEGIN%n" +
                "CREATE CONSTRAINT ON (node:`Person`) ASSERT (node.`name`, node.`surname`) IS NODE KEY;%n" +
                "COMMIT%n" +
                "SCHEMA AWAIT%n");

        static final String EXPECTED_RELATIONSHIP_COMPOUND_CONSTRAINT = String.format(("BEGIN%n" +
                "MATCH (n1:`Person`{`surname`:\"Snow\", `name`:\"John\"}), (n2:`Person`{`surname`:\"Jackson\", `name`:\"Matt\"}) CREATE (n1)-[r:`KNOWS`]->(n2);%n" +
                "COMMIT%n"));

        static final String EXPECTED_INDEX_AWAIT_COMPOUND_CONSTRAINT =  String.format("CALL db.awaitIndex(':`Person`(`name`,`surname`)');%n");

        public static final List<String> EXPECTED_CONSTRAINTS = List.of(
             "CREATE CONSTRAINT SingleUnique FOR (node:Label) REQUIRE (node.prop) IS UNIQUE;",
             "CREATE CONSTRAINT CompositeUnique FOR (node:Label) REQUIRE (node.prop1, node.prop2) IS UNIQUE;",
             "CREATE CONSTRAINT SingleExists FOR (node:Label2) REQUIRE (node.prop) IS NOT NULL;",
             "CREATE CONSTRAINT SingleExistsRel FOR ()-[rel:TYPE2]-() REQUIRE (rel.prop) IS NOT NULL;",
             "CREATE CONSTRAINT SingleNodeKey FOR (node:Label3) REQUIRE (node.prop) IS NODE KEY;",
             "CREATE CONSTRAINT PersonRequiresNamesConstraint FOR (node:Person) REQUIRE (node.name, node.surname) IS NODE KEY;"
        );

        public static final String EXPECTED_NEO4J_SHELL_WITH_COMPOUND_CONSTRAINT = String.format("BEGIN%n" +
                "COMMIT%n" +
                "SCHEMA AWAIT%n" +
                "BEGIN%n" +
                "UNWIND [{surname:\"Snow\", name:\"John\", properties:{}}, {surname:\"Jackson\", name:\"Matt\", properties:{}}, {surname:\"White\", name:\"Jenny\", properties:{}}, {surname:\"Brown\", name:\"Susan\", properties:{}}, {surname:\"Taylor\", name:\"Tom\", properties:{}}] AS row%n" +
                "CREATE (n:Person{surname: row.surname, name: row.name}) SET n += row.properties;%n" +
                "COMMIT%n" +
                "BEGIN%n" +
                "UNWIND [{start: {name:\"John\", surname:\"Snow\"}, end: {name:\"Matt\", surname:\"Jackson\"}, properties:{}}] AS row%n" +
                "MATCH (start:Person{surname: row.start.surname, name: row.start.name})%n" +
                "MATCH (end:Person{surname: row.end.surname, name: row.end.name})%n" +
                "CREATE (start)-[r:KNOWS]->(end) SET r += row.properties;%n" +
                "COMMIT%n");

        public static final String EXPECTED_CYPHER_SHELL_WITH_COMPOUND_CONSTRAINT = String.format(":begin%n" +
                "CREATE CONSTRAINT ON (node:Person) ASSERT (node.name, node.surname) IS NODE KEY;%n" +
                ":commit%n" +
                "CALL db.awaitIndexes(300);%n" +
                ":begin%n" +
                "UNWIND [{surname:\"Snow\", name:\"John\", properties:{}}, {surname:\"Jackson\", name:\"Matt\", properties:{}}, {surname:\"White\", name:\"Jenny\", properties:{}}, {surname:\"Brown\", name:\"Susan\", properties:{}}, {surname:\"Taylor\", name:\"Tom\", properties:{}}] AS row%n" +
                "CREATE (n:Person{surname: row.surname, name: row.name}) SET n += row.properties;%n" +
                ":commit%n" +
                ":begin%n" +
                "UNWIND [{start: {name:\"John\", surname:\"Snow\"}, end: {name:\"Matt\", surname:\"Jackson\"}, properties:{}}] AS row%n" +
                "MATCH (start:Person{surname: row.start.surname, name: row.start.name})%n" +
                "MATCH (end:Person{surname: row.end.surname, name: row.end.name})%n" +
                "CREATE (start)-[r:KNOWS]->(end) SET r += row.properties;%n" +
                ":commit%n");

        public static final String EXPECTED_PLAIN_FORMAT_WITH_COMPOUND_CONSTRAINT = String.format(
                "UNWIND [{surname:\"Snow\", name:\"John\", properties:{}}, {surname:\"Jackson\", name:\"Matt\", properties:{}}, {surname:\"White\", name:\"Jenny\", properties:{}}, {surname:\"Brown\", name:\"Susan\", properties:{}}, {surname:\"Taylor\", name:\"Tom\", properties:{}}] AS row%n" +
                "CREATE (n:Person{surname: row.surname, name: row.name}) SET n += row.properties;%n" +
                "UNWIND [{start: {name:\"John\", surname:\"Snow\"}, end: {name:\"Matt\", surname:\"Jackson\"}, properties:{}}] AS row%n" +
                "MATCH (start:Person{surname: row.start.surname, name: row.start.name})%n" +
                "MATCH (end:Person{surname: row.end.surname, name: row.end.name})%n" +
                "CREATE (start)-[r:KNOWS]->(end) SET r += row.properties;%n");

        static final String EXPECTED_NODES_PARAMS_ODD = String.format(":begin%n" +
                ":param rows => [{_id:4, properties:{age:12}}, {_id:5, properties:{age:4}}]%n" +
                "UNWIND $rows AS row%n" +
                "CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Bar;%n" +
                ":param rows => [{_id:0, properties:{born:date('2018-10-31'), name:\"foo\"}}]%n" +
                "UNWIND $rows AS row%n" +
                "CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Foo;%n" +
                ":commit%n" +
                ":begin%n" +
                ":param rows => [{_id:1, properties:{born:date('2017-09-29'), name:\"foo2\"}}, {_id:2, properties:{born:date('2016-03-12'), name:\"foo3\"}}]%n" +
                "UNWIND $rows AS row%n" +
                "CREATE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`: row._id}) SET n += row.properties SET n:Foo;%n" +
                ":param rows => [{name:\"bar\", properties:{age:42}}]%n" +
                "UNWIND $rows AS row%n" +
                "CREATE (n:Bar{name: row.name}) SET n += row.properties;%n" +
                ":commit%n" +
                ":begin%n" +
                ":param rows => [{name:\"bar2\", properties:{age:44}}, {name:\"bar3\", properties:{age:35}}]%n" +
                "UNWIND $rows AS row%n" +
                "CREATE (n:Bar{name: row.name}) SET n += row.properties;%n" +
                ":param rows => [{name:\"bar4\", properties:{age:36}}]%n" +
                "UNWIND $rows AS row%n" +
                "CREATE (n:Bar{name: row.name}) SET n += row.properties;%n" +
                ":commit%n");

        static final String EXPECTED_DROP_PARAMS_ODD = String.format(":begin%n" +
                "MATCH (n:`UNIQUE IMPORT LABEL`)  WITH n LIMIT %1$d REMOVE n:`UNIQUE IMPORT LABEL` REMOVE n.`UNIQUE IMPORT ID`;%n" +
                ":commit%n" +
                ":begin%n" +
                "MATCH (n:`UNIQUE IMPORT LABEL`)  WITH n LIMIT %1$d REMOVE n:`UNIQUE IMPORT LABEL` REMOVE n.`UNIQUE IMPORT ID`;%n" +
                ":commit%n" +
                ":begin%n" +
                "DROP CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT (node.`UNIQUE IMPORT ID`) IS UNIQUE;%n" +
                ":commit%n", 3);

        public static final String EXPECTED_QUERY_PARAMS_ODD = (EXPECTED_SCHEMA + EXPECTED_NODES_PARAMS_ODD + EXPECTED_RELATIONSHIPS_PARAMS_ODD + EXPECTED_DROP_PARAMS_ODD)
                .replace(NEO4J_SHELL.begin(), CYPHER_SHELL.begin())
                .replace(NEO4J_SHELL.commit(), CYPHER_SHELL.commit())
                .replace(NEO4J_SHELL.schemaAwait(), EXPECTED_INDEXES_AWAIT)
                .replace(NEO4J_SHELL.schemaAwait(), CYPHER_SHELL.schemaAwait());

        static final String EXPECTED_WITHOUT_END_NODE = String.format(EXPECTED_BEGIN_AND_FOO + "COMMIT%n")
                + EXPECTED_SCHEMA_ONLY_START
                + EXPECTED_REL_ONLY
                + EXPECTED_CLEAN_UP;

        public static String convertToCypherShellFormat(String input) {
            return input
                    .replace( NEO4J_SHELL.begin(), CYPHER_SHELL.begin() )
                    .replace( NEO4J_SHELL.commit(), CYPHER_SHELL.commit() )
                    .replace( NEO4J_SHELL.schemaAwait(), EXPECTED_INDEXES_AWAIT )
                    .replace( NEO4J_SHELL.schemaAwait(), CYPHER_SHELL.schemaAwait() );
        }
    }

}
