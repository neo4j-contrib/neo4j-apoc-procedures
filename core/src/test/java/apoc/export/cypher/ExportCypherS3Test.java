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

import apoc.util.TestUtil;
import apoc.util.s3.S3BaseTest;
import apoc.util.s3.S3TestUtil;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.io.IOException;
import java.util.Map;

import static apoc.export.cypher.ExportCypherTest.ExportCypherResults.*;
import static apoc.util.Util.map;
import static apoc.util.s3.S3TestUtil.assertStringFileEquals;
import static org.junit.Assert.*;

public class ExportCypherS3Test extends S3BaseTest {

    private static final Map<String, Object> exportConfig = map("useOptimizations", map("type", "none"), "separateFiles", true, "format", "neo4j-admin");

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();

    @Rule
    public TestName testName = new TestName();

    @Before
    public void setUp() throws Exception {
      ExportCypherTestUtils.setUp(db, testName);
    }

    // -- Whole file test -- //
    @Test
    public void testExportAllCypherDefault() throws Exception {
        String fileName = "all.cypher";
        String s3Url = s3Container.getUrl(fileName);
        TestUtil.testCall(db, "CALL apoc.export.cypher.all($s3,{useOptimizations: { type: 'none'}, format: 'neo4j-shell'})",
                map("s3", s3Url),
                (r) -> assertResults(s3Url, r, "database"));
        assertStringFileEquals(EXPECTED_NEO4J_SHELL, s3Url);
    }

    @Test
    public void testExportAllCypherForCypherShell() throws Exception {
        String fileName = "all.cypher";
        String s3Url = s3Container.getUrl(fileName);
        TestUtil.testCall(db, "CALL apoc.export.cypher.all($s3,$config)",
                map("s3", s3Url, "config", map("useOptimizations", map("type", "none"), "format", "cypher-shell")),
                (r) -> assertResults(s3Url, r, "database"));
        assertStringFileEquals(EXPECTED_CYPHER_SHELL, s3Url);
    }

    @Test
    public void testExportQueryCypherForNeo4j() throws Exception {
        String fileName = "all.cypher";
        String s3Url = s3Container.getUrl(fileName);
        String query = "MATCH (n) OPTIONAL MATCH p = (n)-[r]-(m) RETURN n,r,m";
        TestUtil.testCall(db, "CALL apoc.export.cypher.query($query,$s3,$config)",
                map("s3", s3Url, "query", query, "config", map("useOptimizations", map("type", "none"), "format", "neo4j-shell")), (r) -> {
                });
        assertStringFileEquals(EXPECTED_NEO4J_SHELL, s3Url);
    }

    @Test
    public void testExportGraphCypher() throws Exception {
        String fileName = "graph.cypher";
        String s3Url = s3Container.getUrl(fileName);
        TestUtil.testCall(db, "CALL apoc.graph.fromDB('test',{}) yield graph " +
                        "CALL apoc.export.cypher.graph(graph, $s3,$exportConfig) " +
                        "YIELD nodes, relationships, properties, file, source,format, time " +
                        "RETURN *",
                map("s3", s3Url, "exportConfig", map("useOptimizations", map("type", "none"), "format", "neo4j-shell")),
                (r) -> assertResults(s3Url, r, "graph"));
        assertStringFileEquals(EXPECTED_NEO4J_SHELL, s3Url);
    }

    // -- Separate files tests -- //
    @Test
    public void testExportAllCypherNodes() throws Exception {
        String fileName = "all.cypher";
        String s3Url = s3Container.getUrl(fileName);
        TestUtil.testCall(db, "CALL apoc.export.cypher.all($s3,$exportConfig)",
                map("s3", s3Url, "exportConfig", exportConfig),
                (r) -> assertResults(s3Url, r, "database"));
        getUrlAndAssertEquals(EXPECTED_NODES, "all.nodes.cypher");
    }

    @Test
    public void testExportAllCypherRelationships() throws Exception {
        String fileName = "all.cypher";
        String s3Url = s3Container.getUrl(fileName);
        TestUtil.testCall(db, "CALL apoc.export.cypher.all($s3,$exportConfig)",
                map("s3", s3Url, "exportConfig", exportConfig),
                (r) -> assertResults(s3Url, r, "database"));
        getUrlAndAssertEquals(EXPECTED_RELATIONSHIPS, "all.relationships.cypher");
    }

    @Test
    public void testExportAllCypherSchema() throws Exception {
        String fileName = "all.cypher";
        String s3Url = s3Container.getUrl(fileName);
        TestUtil.testCall(db, "CALL apoc.export.cypher.all($s3,$exportConfig)",
                map("s3", s3Url, "exportConfig", exportConfig),
                (r) -> assertResults(s3Url, r, "database"));
        getUrlAndAssertEquals(EXPECTED_SCHEMA, "all.schema.cypher");
    }

    @Test
    public void testExportAllCypherCleanUp() throws Exception {
        String fileName = "all.cypher";
        String s3Url = s3Container.getUrl(fileName);
        TestUtil.testCall(db, "CALL apoc.export.cypher.all($s3,$exportConfig)",
                map("s3", s3Url, "exportConfig", exportConfig),
                (r) -> assertResults(s3Url, r, "database"));
        getUrlAndAssertEquals(EXPECTED_CLEAN_UP, "all.cleanup.cypher");
    }

    @Test
    public void testExportGraphCypherNodes() throws Exception {
        String fileName = "graph.cypher";
        String s3Url = s3Container.getUrl(fileName);
        TestUtil.testCall(db, "CALL apoc.graph.fromDB('test',{}) yield graph " +
                        "CALL apoc.export.cypher.graph(graph, $s3,$exportConfig) " +
                        "YIELD nodes, relationships, properties, file, source,format, time " +
                        "RETURN *",
                map("s3", s3Url, "exportConfig", exportConfig),
                (r) -> assertResults(s3Url, r, "graph"));
        getUrlAndAssertEquals(EXPECTED_NODES, "graph.nodes.cypher");
    }

    @Test
    public void testExportGraphCypherRelationships() throws Exception {
        String fileName = "graph.cypher";
        String s3Url = s3Container.getUrl(fileName);
        TestUtil.testCall(db, "CALL apoc.graph.fromDB('test',{}) yield graph " +
                        "CALL apoc.export.cypher.graph(graph, $s3,$exportConfig) " +
                        "YIELD nodes, relationships, properties, file, source, format, time " +
                        "RETURN *",
                map("s3", s3Url, "exportConfig", exportConfig),
                (r) -> assertResults(s3Url, r, "graph"));
        getUrlAndAssertEquals(EXPECTED_RELATIONSHIPS, "graph.relationships.cypher");
    }

    @Test
    public void testExportGraphCypherSchema() throws Exception {
        String fileName = "graph.cypher";
        String s3Url = s3Container.getUrl(fileName);
        TestUtil.testCall(db, "CALL apoc.graph.fromDB('test',{}) yield graph " +
                        "CALL apoc.export.cypher.graph(graph, $s3,$exportConfig) " +
                        "YIELD nodes, relationships, properties, file, source,format, time " +
                        "RETURN *",
                map("s3", s3Url, "exportConfig", exportConfig),
                (r) -> assertResults(s3Url, r, "graph"));
        getUrlAndAssertEquals(EXPECTED_SCHEMA, "graph.schema.cypher");
    }

    @Test
    public void testExportGraphCypherCleanUp() throws Exception {
        String fileName = "graph.cypher";
        String s3Url = s3Container.getUrl(fileName);
        TestUtil.testCall(db, "CALL apoc.graph.fromDB('test',{}) yield graph " +
                        "CALL apoc.export.cypher.graph(graph, $s3,$exportConfig) " +
                        "YIELD nodes, relationships, properties, file, source,format, time " +
                        "RETURN *",
                map("s3", s3Url, "exportConfig", exportConfig),
                (r) -> assertResults(s3Url, r, "graph"));
        getUrlAndAssertEquals(EXPECTED_CLEAN_UP, "graph.cleanup.cypher");
    }

    private void assertResults(String fileName, Map<String, Object> r, final String source) {
        assertEquals(3L, r.get("nodes"));
        assertEquals(1L, r.get("relationships"));
        assertEquals(6L, r.get("properties"));
        assertEquals(fileName, r.get("file"));
        assertEquals(source + ": nodes(3), rels(1)", r.get("source"));
        assertEquals("cypher", r.get("format"));
        assertTrue("Should get time greater than 0", ((long) r.get("time")) >= 0);
    }

    @Test
    public void testExportQueryCypherPlainFormat() throws Exception {
        String fileName = "all.cypher";
        String s3Url = s3Container.getUrl(fileName);
        String query = "MATCH (n) OPTIONAL MATCH p = (n)-[r]-(m) RETURN n,r,m";
        TestUtil.testCall(db, "CALL apoc.export.cypher.query($query,$s3,$config)",
                map("s3", s3Url, "query", query, "config", map("useOptimizations", map("type", "none"), "format", "plain")), (r) -> {
                });
        assertStringFileEquals(EXPECTED_PLAIN, s3Url);
    }

    @Test
    public void testExportQueryCypherFormatUpdateAll() throws Exception {
        String fileName = "all.cypher";
        String s3Url = s3Container.getUrl(fileName);
        String query = "MATCH (n) OPTIONAL MATCH p = (n)-[r]-(m) RETURN n,r,m";
        TestUtil.testCall(db, "CALL apoc.export.cypher.query($query,$s3,$config)",
                map("s3", s3Url, "query", query, "config", map("useOptimizations", map("type", "none"), "format", "neo4j-shell", "cypherFormat", "updateAll")), (r) -> {
                });
        assertStringFileEquals(EXPECTED_NEO4J_MERGE, s3Url);
    }

    @Test
    public void testExportQueryCypherFormatAddStructure() throws Exception {
        String fileName = "all.cypher";
        String s3Url = s3Container.getUrl(fileName);
        String query = "MATCH (n) OPTIONAL MATCH p = (n)-[r]-(m) RETURN n,r,m";
        TestUtil.testCall(db, "CALL apoc.export.cypher.query($query,$s3,$config)",
                map("s3", s3Url, "query", query, "config", map("useOptimizations", map("type", "none"), "format", "neo4j-shell", "cypherFormat", "addStructure")), (r) -> {
                });
        assertStringFileEquals(EXPECTED_NODES_MERGE_ON_CREATE_SET + EXPECTED_SCHEMA_EMPTY + EXPECTED_RELATIONSHIPS + EXPECTED_CLEAN_UP_EMPTY, s3Url);
    }

    @Test
    public void testExportQueryCypherFormatUpdateStructure() throws Exception {
        String fileName = "all.cypher";
        String s3Url = s3Container.getUrl(fileName);
        String query = "MATCH (n) OPTIONAL MATCH p = (n)-[r]-(m) RETURN n,r,m";
        TestUtil.testCall(db, "CALL apoc.export.cypher.query($query,$s3,$config)",
                map("s3", s3Url, "query", query, "config", map("useOptimizations", map("type", "none"), "format", "neo4j-shell", "cypherFormat", "updateStructure")), (r) -> {
                });
        assertStringFileEquals(EXPECTED_NODES_EMPTY + EXPECTED_SCHEMA_EMPTY + EXPECTED_RELATIONSHIPS_MERGE_ON_CREATE_SET + EXPECTED_CLEAN_UP_EMPTY, s3Url);
    }

    @Test
    public void testExportSchemaCypher() throws Exception {
        String fileName = "onlySchema.cypher";
        String s3Url = s3Container.getUrl(fileName);
        TestUtil.testCall(db, "CALL apoc.export.cypher.schema($s3,$exportConfig)", map("s3", s3Url, "exportConfig", exportConfig), (r) -> {
        });
        assertStringFileEquals(EXPECTED_ONLY_SCHEMA_NEO4J_SHELL, s3Url);
    }

    @Test
    public void testExportSchemaCypherShell() throws Exception {
        String fileName = "onlySchema.cypher";
        String s3Url = s3Container.getUrl(fileName);
        TestUtil.testCall(db, "CALL apoc.export.cypher.schema($s3,$exportConfig)",
                map("s3", s3Url, "exportConfig", map("useOptimizations", map("type", "none"), "format", "cypher-shell")),
                (r) -> {
                });
        assertStringFileEquals(EXPECTED_ONLY_SCHEMA_CYPHER_SHELL, s3Url);
    }

    @Test
    public void testExportCypherNodePoint() throws IOException {
        db.executeTransactionally("CREATE (f:Test {name:'foo'," +
                "place2d:point({ x: 2.3, y: 4.5 })," +
                "place3d1:point({ x: 2.3, y: 4.5 , z: 1.2})})" +
                "-[:FRIEND_OF {place2d:point({ longitude: 56.7, latitude: 12.78 })}]->" +
                "(:Bar {place3d:point({ longitude: 12.78, latitude: 56.7, height: 100 })})");
        String fileName = "temporalPoint.cypher";
        String s3Url = s3Container.getUrl(fileName);
        String query = "MATCH (n:Test)-[r]-(m) RETURN n,r,m";
        TestUtil.testCall(db, "CALL apoc.export.cypher.query($query,$s3,$config)",
                map("s3", s3Url, "query", query, "config", map("useOptimizations", map("type", "none"), "format", "neo4j-shell")),
                (r) -> {
                });
        assertStringFileEquals(EXPECTED_CYPHER_POINT, s3Url);
    }

    @Test
    public void testExportCypherNodeDate() throws IOException {
        db.executeTransactionally("CREATE (f:Test {name:'foo', " +
                "date:date('2018-10-30'), " +
                "datetime:datetime('2018-10-30T12:50:35.556+0100'), " +
                "localTime:localdatetime('20181030T19:32:24')})" +
                "-[:FRIEND_OF {date:date('2018-10-30')}]->" +
                "(:Bar {datetime:datetime('2018-10-30T12:50:35.556')})");
        String fileName = "temporalDate.cypher";
        String s3Url = s3Container.getUrl(fileName);
        String query = "MATCH (n:Test)-[r]-(m) RETURN n,r,m";
        TestUtil.testCall(db, "CALL apoc.export.cypher.query($query,$s3,$config)",
                map("s3", s3Url, "query", query, "config", map("useOptimizations", map("type", "none"), "format", "neo4j-shell")),
                (r) -> {
                });
        assertStringFileEquals(EXPECTED_CYPHER_DATE, s3Url);
    }

    @Test
    public void testExportCypherNodeTime() throws IOException {
        db.executeTransactionally("CREATE (f:Test {name:'foo', " +
                "local:localtime('12:50:35.556')," +
                "t:time('125035.556+0100')})" +
                "-[:FRIEND_OF {t:time('125035.556+0100')}]->" +
                "(:Bar {datetime:datetime('2018-10-30T12:50:35.556+0100')})");
        String fileName = "temporalTime.cypher";
        String s3Url = s3Container.getUrl(fileName);
        String query = "MATCH (n:Test)-[r]-(m) RETURN n,r,m";
        TestUtil.testCall(db, "CALL apoc.export.cypher.query($query,$s3,$config)",
                map("s3", s3Url, "query", query, "config", map("useOptimizations", map("type", "none"), "format", "neo4j-shell")),
                (r) -> {
                });
        assertStringFileEquals(EXPECTED_CYPHER_TIME, s3Url);
    }

    @Test
    public void testExportCypherNodeDuration() throws IOException {
        db.executeTransactionally("CREATE (f:Test {name:'foo', " +
                "duration:duration('P5M1.5D')})" +
                "-[:FRIEND_OF {duration:duration('P5M1.5D')}]->" +
                "(:Bar {duration:duration('P5M1.5D')})");
        String fileName = "temporalDuration.cypher";
        String s3Url = s3Container.getUrl(fileName);
        String query = "MATCH (n:Test)-[r]-(m) RETURN n,r,m";
        TestUtil.testCall(db, "CALL apoc.export.cypher.query($query,$s3,$config)",
                map("s3", s3Url, "query", query, "config", map("useOptimizations", map("type", "none"), "format", "neo4j-shell")),
                (r) -> {
                });
        assertStringFileEquals(EXPECTED_CYPHER_DURATION, s3Url);
    }

    @Test
    public void testExportWithAscendingLabels() throws IOException {
        db.executeTransactionally("CREATE (f:User:User1:User0:User12 {name:'Alan'})");
        String fileName = "ascendingLabels.cypher";
        String s3Url = s3Container.getUrl(fileName);
        String query = "MATCH (f:User) WHERE f.name='Alan' RETURN f";
        TestUtil.testCall(db, "CALL apoc.export.cypher.query($query,$s3,$config)",
                map("s3", s3Url, "query", query, "config", map("useOptimizations", map("type", "none"), "format", "neo4j-shell")),
                (r) -> {
                });
        assertStringFileEquals(EXPECTED_CYPHER_LABELS_ASCENDEND, s3Url);
    }

    @Test
    public void testExportAllCypherDefaultWithUnwindBatchSizeOptimized() throws Exception {
        String fileName = "allDefaultOptimized.cypher";
        String s3Url = s3Container.getUrl(fileName);
        TestUtil.testCall(db, "CALL apoc.export.cypher.all($s3,{useOptimizations: { type: 'unwind_batch', unwindBatchSize: 2}, format: 'neo4j-shell'})", map("s3", s3Url),
                (r) -> assertResultsOptimized(s3Url, r));
        assertStringFileEquals(EXPECTED_NEO4J_OPTIMIZED_BATCH_SIZE, s3Url);
    }

    @Test
    public void testExportAllCypherDefaultOptimized() throws Exception {
        String fileName = "allDefaultOptimized.cypher";
        String s3Url = s3Container.getUrl(fileName);
        TestUtil.testCall(db, "CALL apoc.export.cypher.all($s3, $exportConfig)", map("s3", s3Url, "exportConfig", map("format", "neo4j-shell")),
                (r) -> assertResultsOptimized(s3Url, r));
        assertStringFileEquals(EXPECTED_NEO4J_OPTIMIZED, s3Url);
    }

    @Test
    public void testExportAllCypherDefaultSeparatedFilesOptimized() throws Exception {
        String fileName = "allDefaultOptimized.cypher";
        String s3Url = s3Container.getUrl(fileName);
        TestUtil.testCall(db, "CALL apoc.export.cypher.all($s3, $exportConfig)",
                map("s3", s3Url, "exportConfig", map("separateFiles", true, "format", "neo4j-shell")),
                (r) -> assertResultsOptimized(s3Url, r));
        getUrlAndAssertEquals(EXPECTED_NODES_OPTIMIZED, "allDefaultOptimized.nodes.cypher");
        getUrlAndAssertEquals(EXPECTED_RELATIONSHIPS_OPTIMIZED, "allDefaultOptimized.relationships.cypher");
        getUrlAndAssertEquals(EXPECTED_SCHEMA, "allDefaultOptimized.schema.cypher");
        getUrlAndAssertEquals(EXPECTED_CLEAN_UP, "allDefaultOptimized.cleanup.cypher");
    }

    @Test
    public void testExportAllCypherCypherShellWithUnwindBatchSizeOptimized() throws Exception {
        String fileName = "allCypherShellOptimized.cypher";
        String s3Url = s3Container.getUrl(fileName);
        TestUtil.testCall(db, "CALL apoc.export.cypher.all($s3,{format:'cypher-shell', useOptimizations: {type: 'unwind_batch'}})",
                map("s3", s3Url),
                (r) -> assertResultsOptimized(s3Url, r));
        assertStringFileEquals(EXPECTED_CYPHER_SHELL_OPTIMIZED_BATCH_SIZE, s3Url);
    }

    @Test
    public void testExportAllCypherCypherShellOptimized() throws Exception {
        String fileName = "allCypherShellOptimized.cypher";
        String s3Url = s3Container.getUrl(fileName);
        TestUtil.testCall(db, "CALL apoc.export.cypher.all($s3,{format:'cypher-shell'})",
                map("s3", s3Url),
                (r) -> assertResultsOptimized(s3Url, r));
        assertStringFileEquals(EXPECTED_CYPHER_SHELL_OPTIMIZED, s3Url);
    }

    @Test
    public void testExportAllCypherPlainWithUnwindBatchSizeOptimized() throws Exception {
        String fileName = "allPlainOptimized.cypher";
        String s3Url = s3Container.getUrl(fileName);
        TestUtil.testCall(db, "CALL apoc.export.cypher.all($s3,{format:'plain', useOptimizations: { type: 'unwind_batch', unwindBatchSize: 2}})",
                map("s3", s3Url),
                (r) -> assertResultsOptimized(s3Url, r));
        assertStringFileEquals(EXPECTED_PLAIN_OPTIMIZED_BATCH_SIZE, s3Url);
    }

    @Test
    public void testExportAllCypherPlainAddStructureWithUnwindBatchSizeOptimized() throws Exception {
        String fileName = "allPlainAddStructureOptimized.cypher";
        String s3Url = s3Container.getUrl(fileName);
        TestUtil.testCall(db, "CALL apoc.export.cypher.all($s3,{format:'plain', cypherFormat: 'addStructure', useOptimizations: { type: 'unwind_batch', unwindBatchSize: 2}})",
                map("s3", s3Url), (r) -> assertResultsOptimized(s3Url, r));
        assertStringFileEquals(EXPECTED_PLAIN_ADD_STRUCTURE_UNWIND, s3Url);
    }

    @Test
    public void testExportAllCypherPlainUpdateStructureWithUnwindBatchSizeOptimized() throws Exception {
        String fileName = "allPlainUpdateStructureOptimized.cypher";
        String s3Url = s3Container.getUrl(fileName);
        TestUtil.testCall(db, "CALL apoc.export.cypher.all($s3,{format:'plain', cypherFormat: 'updateStructure', useOptimizations: { type: 'unwind_batch', unwindBatchSize: 2}})",
                map("s3", s3Url), (r) -> {
                    assertEquals(0L, r.get("nodes"));
                    assertEquals(2L, r.get("relationships"));
                    assertEquals(2L, r.get("properties"));
                    assertEquals(s3Url, r.get("file"));
                    assertEquals("cypher", r.get("format"));
                    assertTrue("Should get time greater than 0", ((long) r.get("time")) >= 0);
                });
        assertStringFileEquals(EXPECTED_PLAIN_UPDATE_STRUCTURE_UNWIND, s3Url);
    }

    @Test
    public void testExportAllCypherPlainUpdateAllWithUnwindBatchSizeOptimized() throws Exception {
        String fileName = "allPlainUpdateAllOptimized.cypher";
        String s3Url = s3Container.getUrl(fileName);
        TestUtil.testCall(db, "CALL apoc.export.cypher.all($s3,{format:'plain', cypherFormat: 'updateAll', useOptimizations: { type: 'unwind_batch', unwindBatchSize: 2}})",
                map("s3", s3Url), (r) -> assertResultsOptimized(s3Url, r));
        assertStringFileEquals(EXPECTED_UPDATE_ALL_UNWIND, s3Url);
    }

    @Test
    public void testExportQueryCypherShellWithUnwindBatchSizeWithBatchSizeOptimized() throws Exception {
        String fileName = "allPlainOptimized.cypher";
        String s3Url = s3Container.getUrl(fileName);
        TestUtil.testCall(db, "CALL apoc.export.cypher.all($s3,{format:'cypher-shell', useOptimizations: { type: 'unwind_batch', unwindBatchSize: 2}, batchSize: 2})",
                map("s3", s3Url),
                (r) -> assertResultsOptimized(s3Url, r));
        assertStringFileEquals(EXPECTED_QUERY_CYPHER_SHELL_OPTIMIZED_UNWIND, s3Url);
    }

    @Test
    public void testExportQueryCypherShellWithUnwindBatchSizeWithBatchSizeOddDataset() throws Exception {
        String fileName = "allPlainOdd.cypher";
        String s3Url = s3Container.getUrl(fileName);
        TestUtil.testCall(db, "CALL apoc.export.cypher.all($s3,{format:'cypher-shell', useOptimizations: { type: 'unwind_batch', unwindBatchSize: 2}, batchSize: 2})",
                map("s3", s3Url), (r) -> assertResultsOdd(s3Url, r));
        assertStringFileEquals(EXPECTED_QUERY_CYPHER_SHELL_OPTIMIZED_ODD, s3Url);
    }

    @Test
    public void testExportQueryCypherShellUnwindBatchParamsWithOddDataset() throws Exception {
        String fileName = "allPlainOdd.cypher";
        String s3Url = s3Container.getUrl(fileName);
        TestUtil.testCall(db, "CALL apoc.export.cypher.all($s3,{format:'cypher-shell', useOptimizations: { type: 'unwind_batch_params', unwindBatchSize: 2}, batchSize:2})",
                map("s3", s3Url),
                (r) -> assertResultsOdd(s3Url, r));
        assertStringFileEquals(EXPECTED_QUERY_CYPHER_SHELL_PARAMS_OPTIMIZED_ODD, s3Url);
    }

    @Test
    public void testExportQueryCypherShellUnwindBatchParamsWithOddBatchSizeOddDataset() throws Exception {
        db.executeTransactionally("CREATE (:Bar {name:'bar3',age:35}), (:Bar {name:'bar4',age:36})");
        String fileName = "allPlainOddNew.cypher";
        String s3Url = s3Container.getUrl(fileName);
        TestUtil.testCall(db, "CALL apoc.export.cypher.all($s3,{format:'cypher-shell', useOptimizations: { type: 'unwind_batch_params', unwindBatchSize: 2}, batchSize:3})",
                map("s3", s3Url),
                (r) -> {
                });
        db.executeTransactionally("MATCH (n:Bar {name:'bar3',age:35}), (n1:Bar {name:'bar4',age:36}) DELETE n, n1");
        assertStringFileEquals(EXPECTED_QUERY_PARAMS_ODD, s3Url);
    }

    private void getUrlAndAssertEquals(String expected, String fileName) {
        final String urlFile = s3Container.getUrl(fileName);
        S3TestUtil.assertStringFileEquals(expected, urlFile);
    }

    private void assertResultsOptimized(String fileName, Map<String, Object> r) {
        assertEquals(7L, r.get("nodes"));
        assertEquals(2L, r.get("relationships"));
        assertEquals(13L, r.get("properties"));
        assertEquals(fileName, r.get("file"));
        assertEquals("database" + ": nodes(7), rels(2)", r.get("source"));
        assertEquals("cypher", r.get("format"));
        assertTrue("Should get time greater than 0", ((long) r.get("time")) >= 0);
    }

    private void assertResultsOdd(String fileName, Map<String, Object> r) {
        assertEquals(7L, r.get("nodes"));
        assertEquals(1L, r.get("relationships"));
        assertEquals(13L, r.get("properties"));
        assertEquals(fileName, r.get("file"));
        assertEquals("database" + ": nodes(7), rels(1)", r.get("source"));
        assertEquals("cypher", r.get("format"));
        assertTrue("Should get time greater than 0", ((long) r.get("time")) >= 0);
    }

}
