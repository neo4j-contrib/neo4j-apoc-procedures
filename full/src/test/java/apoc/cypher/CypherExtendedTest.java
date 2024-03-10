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
package apoc.cypher;

import static apoc.ApocConfig.APOC_IMPORT_FILE_ENABLED;
import static apoc.ApocConfig.apocConfig;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testCallCount;
import static apoc.util.TestUtil.testCallEmpty;
import static apoc.util.TestUtil.testResult;
import static apoc.util.Util.map;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasEntry;
import static org.junit.Assert.*;
import static org.neo4j.driver.internal.util.Iterables.count;

import apoc.text.Strings;
import apoc.util.TestUtil;
import apoc.util.Util;
import apoc.util.Utils;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;
import org.apache.commons.io.FileUtils;
import org.junit.*;
import org.junit.rules.ExpectedException;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.IndexType;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

/**
 * @author mh
 * @since 08.05.16
 */
public class CypherExtendedTest {
    public static final String IMPORT_DIR = "src/test/resources";

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(GraphDatabaseSettings.allow_file_urls, true)
            .withSetting(
                    GraphDatabaseSettings.load_csv_file_url_root,
                    new File(IMPORT_DIR).toPath().toAbsolutePath());

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @BeforeClass
    public static void setUp() {
        apocConfig().setProperty(APOC_IMPORT_FILE_ENABLED, true);
        TestUtil.registerProcedure(
                db,
                Cypher.class,
                CypherExtended.class,
                Utils.class,
                CypherFunctions.class,
                Timeboxed.class,
                Strings.class);
    }

    @AfterClass
    public static void teardown() {
        db.shutdown();
    }

    @After
    public void clearDB() {
        db.executeTransactionally("MATCH (n) DETACH DELETE n");
        clearSchema();
    }

    private void clearSchema() {
        try (Transaction tx = db.beginTx()) {
            tx.schema().getConstraints().forEach(ConstraintDefinition::drop);
            tx.schema().getIndexes().forEach(IndexDefinition::drop);
            tx.commit();
        }
    }

    @Test
    public void testParallelWithParamListInQuery() {
        int numPeople = 9999;
        db.executeTransactionally(
                "UNWIND range(0, $numPeople) as idx CREATE (:Person {name: idx})", map("numPeople", numPeople));

        final String suffix = " - suffix";
        final Set<String> expected =
                IntStream.rangeClosed(0, numPeople).boxed().map(i -> i + suffix).collect(Collectors.toSet());
        testResult(
                db,
                "MATCH (p:Person) WITH collect(p) as people \n"
                        + "CALL apoc.cypher.parallel2('RETURN a.name + $t as title', {a: people, t: $suffix}, 'a')\n"
                        + " YIELD value RETURN value.title as title",
                map("suffix", suffix),
                r -> {
                    final Set<String> actual = Iterators.asSet(r.columnAs("title"));
                    assertEquals(expected, actual);
                });

        testResult(
                db,
                "MATCH (p:Person) WITH collect(p) as people \n"
                        + "CALL apoc.cypher.parallel('RETURN a.name + $t as title',{a: people, t: $suffix},'a')\n"
                        + " yield value RETURN value.title as title",
                map("suffix", suffix),
                r -> {
                    final Set<String> actual = Iterators.asSet(r.columnAs("title"));
                    assertEquals(expected, actual);
                });
    }

    @Test
    public void testParallel() throws Exception {
        int size = 10_000;
        testResult(
                db,
                "CALL apoc.cypher.parallel('UNWIND range(0,9) as b RETURN b',{a:range(1,$size)},'a')",
                map("size", size),
                r -> assertEquals(size * 10, Iterators.count(r)));
    }

    @Test
    public void testMapParallel() throws Exception {
        int size = 10_000;
        testResult(
                db,
                "CALL apoc.cypher.mapParallel('UNWIND range(0,9) as b RETURN b',{},range(1,$size))",
                map("size", size),
                r -> assertEquals(size * 10, Iterators.count(r)));
    }

    @Test
    @Ignore("flaky")
    public void testMapParallel2() throws Exception {
        int size = 10_000;
        testResult(
                db,
                "CALL apoc.cypher.mapParallel2('UNWIND range(0,9) as b RETURN b',{},range(1,$size),10)",
                map("size", size),
                r -> assertEquals(size * 10, Iterators.count(r)));
    }

    @Test
    public void testParallel2() throws Exception {
        int size = 10_0000;
        List<Long> list = new ArrayList<>(size);
        for (int i = 0; i < size; i++) list.add(3L);
        testCall(
                db,
                "CALL apoc.cypher.parallel2('RETURN a + 7 as b',{a:$list},'a') YIELD value RETURN sum(value.b) as b",
                map("list", list),
                r -> {
                    assertEquals(size * 10L, r.get("b"));
                });
    }

    private long toLong(Object value) {
        return Util.toLong(value);
    }

    @Test
    public void testRunFile() throws Exception {
        testResult(db, "CALL apoc.cypher.runFile('create_delete.cypher')", r -> {
            assertCreateDeleteFile(r);
            assertFalse(r.hasNext());
        });
    }

    @Test
    public void testRunFileWithAutoTransaction() {
        final int expectedCount = 2000;
        testCall(db, "CALL apoc.cypher.runFile('in_transaction.cypher')", row -> {
            assertEquals(-1L, row.get("row"));
            Map result = (Map) row.get("result");
            List.of("nodesCreated", "labelsAdded", "propertiesSet")
                    .forEach(item -> assertEquals(expectedCount, result.get(item)));
        });

        testCallCount(db, "MATCH (n:AutoTransaction) RETURN n", Collections.emptyMap(), expectedCount);
    }

    @Test
    public void testRunWithPeriodic() throws Exception {
        testResult(db, "CALL apoc.cypher.runFile('periodic.cypher')", r -> {
            Map<String, Object> row = r.next();
            assertEquals(-1L, row.get("row"));
            Map result = (Map) row.get("result");
            assertEquals(1L, toLong(result.get("nodesCreated")));
            assertEquals(1L, toLong(result.get("labelsAdded")));
            assertEquals(2L, toLong(result.get("propertiesSet")));
            assertEquals(false, r.hasNext());
        });
    }

    @Test
    public void testRunFileWithSchema() throws Exception {
        testResult(db, "CALL apoc.cypher.runFile('schema_create.cypher')", r -> {
            Map<String, Object> row = r.next();
            assertEquals(-1L, row.get("row"));
            Map result = (Map) row.get("result");
            assertEquals(0L, toLong(result.get("indexesAdded")));
            assertEquals(1L, toLong(result.get("nodesCreated")));
            assertEquals(1L, toLong(result.get("propertiesSet")));
            assertEquals(false, r.hasNext());
        });
    }

    @Test
    public void testRunFileWithResults() throws Exception {
        testResult(db, "CALL apoc.cypher.runFile('create.cypher')", r -> {
            Map<String, Object> row = r.next();
            assertEquals(row.get("row"), ((Map) row.get("result")).get("id"));
            row = r.next();
            assertEquals(row.get("row"), ((Map) row.get("result")).get("id"));
            row = r.next();
            assertEquals(row.get("row"), ((Map) row.get("result")).get("id"));
            row = r.next();
            assertEquals(-1L, row.get("row"));
            Map result = (Map) row.get("result");
            assertEquals(3L, toLong(result.get("nodesCreated")));
            assertEquals(3L, toLong(result.get("labelsAdded")));
            assertEquals(3L, toLong(result.get("propertiesSet")));

            row = r.next();
            result = (Map) row.get("result");
            assertEquals(-1L, row.get("row"));
            assertEquals(3L, toLong(result.get("nodesDeleted")));
            assertEquals(false, r.hasNext());
        });
    }

    @Test
    public void shouldNotFailWithTransactionErrorWithMapParallel2() {
        // Sometimes it was green even without `db.beginTx()` modification
        db.executeTransactionally(
                "UNWIND range(1, 100) as i create (p:Page {title: i})-[:Link]->(p1:Page1)<-[:Link]-(p2:Page2 {title: 'myTitle'})");
        testResult(
                db,
                "MATCH (p:Page) WITH collect(p) as pages\n"
                        + "CALL apoc.cypher.mapParallel2(\"MATCH (_)-[:Link]->(p1)<-[:Link]-(p2)\n"
                        + "RETURN  p2.title as title\", {}, pages, 1) yield value\n"
                        + "RETURN value.title limit 5",
                r -> assertEquals(5, Iterators.count(r)));
    }

    @Test
    public void testIssue3751MapParallel2() {
        int expected = 500;
        db.executeTransactionally("UNWIND range(1, $int) as id CREATE (:Polling {id: id})", Map.of("int", expected));

        // the error is flaky, so we need to run the query several times to replicate it
        for (int i = 0; i < 30; i++) {
            testCallCount(
                    db,
                    "MATCH (n:Polling) WITH collect({childD: n}) as params  \n"
                            + "CALL apoc.cypher.mapParallel2(\" WITH _.childD as childD RETURN childD\", {}, params, 6, 10)\n"
                            + "YIELD value RETURN value",
                    Map.of(),
                    expected);
        }

        // Check that `SHOW TRANSACTIONS` just returns itself
        String showTransactionsQuery = "SHOW TRANSACTIONS";
        testCall(db, showTransactionsQuery, r -> assertEquals(showTransactionsQuery, r.get("currentQuery")));
    }

    @Test
    public void testIssue3751RunFiles() {
        int numEntities = 500;
        db.executeTransactionally("UNWIND range(1, $int) as id CREATE (:Polling {id: id})", Map.of("int", numEntities));

        for (int i = 0; i < 30; i++) {
            int numFiles = 30;
            List<String> files = Collections.nCopies(numFiles, "parallel.cypher");

            int expected = numEntities * numFiles;
            testCallCount(
                    db, "CALL apoc.cypher.runFiles($files, {statistics: false})", Map.of("files", files), expected);
        }
    }

    @Test
    public void testIssue3751RunSchemaFiles() throws IOException {
        for (int i = 0; i < 15; i++) {
            int numFiles = 10;
            List<File> files = new ArrayList<>();

            for (int fileIdx = 0; fileIdx < numFiles; fileIdx++) {
                String id = i + "" + fileIdx;
                File file = new File(IMPORT_DIR, "schema" + id);

                String content = String.format(
                        "CREATE INDEX index%1$s FOR (n:Person%1$s) ON (n.name%1$s);\n"
                                + "CREATE INDEX secondIndex%1$s FOR (n:Foo%1$s) ON (n.bar%1$s);\n"
                                + "CREATE INDEX thirdIndex%1$s FOR (n:Ajeje%1$s) ON (n.brazorf%1$s);\n",
                        id);

                FileUtils.writeStringToFile(file, content, StandardCharsets.UTF_8);

                files.add(file);
            }

            // 4 is the number of indexes
            int expected = numFiles * 3;
            List<String> fileNames = files.stream().map(File::getName).collect(Collectors.toList());
            testCallCount(
                    db,
                    "CALL apoc.cypher.runSchemaFiles($files, {statistics: true})",
                    Map.of("files", fileNames),
                    expected);

            files.forEach(File::delete);
        }
    }

    @Test
    public void testRunFileWithParameters() throws Exception {
        testResult(
                db,
                "CALL apoc.cypher.runFile('parameterized.cypher', {statistics:false,parameters:{foo:123,bar:'baz'}})",
                r -> {
                    assertTrue("first row", r.hasNext());
                    Map<String, Object> result = (Map<String, Object>) r.next().get("result");
                    assertEquals(result.toString(), 1, result.size());
                    assertThat(result, hasEntry("one", 123L));
                    assertTrue("second row", r.hasNext());
                    result = (Map<String, Object>) r.next().get("result");
                    assertEquals(result.toString(), 1, result.size());
                    assertThat(result, hasEntry("two", "baz"));
                    assertTrue("third row", r.hasNext());
                    result = (Map<String, Object>) r.next().get("result");
                    assertEquals(result.toString(), 2, result.size());
                    assertThat(result, hasEntry("foo", 123L));
                    assertThat(result, hasEntry("bar", "baz"));
                    assertFalse("fourth row", r.hasNext());
                });
    }

    @Test
    public void testRunFilesMultiple() throws Exception {
        // The execution of both these files should happen sequentially
        // There was a bug before that made both executions to
        // interleave and at some point we were seeing nodesDeleted = 4
        testResult(
                db, "CALL apoc.cypher.runFiles(['create_with_sleep.cypher', 'create_delete_with_sleep.cypher'])", r -> {
                    Map<String, Object> row = r.next();
                    assertEquals(row.get("row"), ((Map) row.get("result")).get("id"));
                    row = r.next();
                    assertEquals(row.get("row"), ((Map) row.get("result")).get("id"));
                    row = r.next();
                    assertEquals(row.get("row"), ((Map) row.get("result")).get("id"));
                    row = r.next();
                    assertEquals(-1L, row.get("row"));
                    Map result = (Map) row.get("result");
                    assertEquals(3L, toLong(result.get("nodesCreated")));
                    assertEquals(3L, toLong(result.get("labelsAdded")));
                    assertEquals(3L, toLong(result.get("propertiesSet")));
                    row = r.next();
                    result = (Map) row.get("result");
                    assertEquals(3L, toLong(result.get("nodesDeleted")));
                    row = r.next();
                    result = (Map) row.get("result");
                    assertEquals(-1L, row.get("row"));
                    assertEquals(1L, toLong(result.get("nodesCreated")));
                    assertEquals(1L, toLong(result.get("labelsAdded")));
                    assertEquals(1L, toLong(result.get("propertiesSet")));
                    row = r.next();
                    result = (Map) row.get("result");
                    assertEquals(-1L, row.get("row"));
                    assertEquals(1L, toLong(result.get("nodesDeleted")));
                    assertEquals(false, r.hasNext());
                });
    }

    @Test
    public void testParallelWithFailingStatement() {
        String call = "MATCH (t:Test {a: \"\"}) WITH collect(t) as nodes\n"
                + "call apoc.cypher.parallel(\"RETURN date(n.a)\", {n: nodes}, \"n\")\n"
                + "yield value return value";
        testFailingParallelCommon(call);
    }

    @Test
    public void testParallel2WithFailingStatement() {
        String call = "MATCH (t:Test {a: \"\"}) WITH collect(t) as nodes\n"
                + "call apoc.cypher.parallel2(\"RETURN date(n.a)\", {n: nodes}, \"n\")\n"
                + "yield value return value";
        testFailingParallelCommon(call);
    }

    @Test
    public void testMapParallelWithFailingStatement() {
        String call = "MATCH (t:Test) WITH collect(t) as nodes\n"
                + "CALL apoc.cypher.mapParallel(\"MATCH (_) RETURN date(_.a)\", {}, nodes)\n"
                + "YIELD value RETURN value";
        testFailingParallelCommon(call);
    }

    private static void testFailingParallelCommon(String query) {
        db.executeTransactionally("CREATE (t:Test {a: ''});");

        QueryExecutionException error =
                assertThrows(QueryExecutionException.class, () -> testCall(db, query, (r) -> {}));

        assertThat(error.getMessage(), containsString("Text cannot be parsed to a Date"));
    }

    @Test
    public void testRunFileWithFailingStatement() {
        String failingFile = "wrong_statements_runtime.cypher";
        String cypherError = "already exists with label `Fail` and property `foo` = 1";
        testRunFailingFileCommon(failingFile, cypherError);
    }

    @Test
    public void testRunFileWithFailingPeriodicStatement() {
        String failingFile = "wrong_call_in_transactions.cypher";
        String cypherError = "already exists with label `Fail` and property `foo` = 1";
        testRunFailingFileCommon(failingFile, cypherError);
    }

    @Test
    public void testRunFileWithFailingExplain() {
        // error during CypherExtended.isSchemaOperation method
        String failingFile = "wrong_statements.cypher";
        String cypherError = "Invalid input ')': expected";
        testRunFailingFileCommon(failingFile, cypherError);
    }

    private void testRunFailingFileCommon(String failingFile, String cypherError) {
        db.executeTransactionally("CREATE CONSTRAINT FOR (n:Fail) REQUIRE n.foo IS UNIQUE");
        db.executeTransactionally("CREATE (n:Fail {foo: 1})");

        // the failed file is skipped
        testCallEmpty(db, "CALL apoc.cypher.runFile($file)", Map.of("file", failingFile));

        // the failed file produces an "error" row
        testCall(db, "CALL apoc.cypher.runFile($file, {reportError: true})", Map.of("file", failingFile), r -> {
            assertErrorResult(cypherError, r);
        });
    }

    @Test
    public void testRunSchemaFileWithFailingStatement() {
        db.executeTransactionally("CREATE CONSTRAINT uniqueConstraint FOR (n:Person) REQUIRE n.name IS UNIQUE");
        String failingFile = "constraints.cypher";

        // the failed file is skipped
        testCallEmpty(db, "CALL apoc.cypher.runSchemaFile($file)", Map.of("file", failingFile));

        // the failed file produces an "error" row
        testCall(db, "CALL apoc.cypher.runSchemaFile($file, {reportError: true})", Map.of("file", failingFile), row -> {
            String cypherError = "Error in `constraints.cypher`:\nConstraint already exists";
            assertErrorResult(cypherError, row);
        });
    }

    @Test
    public void testRunSchemaFileWithFailingExplain() {
        // error during CypherExtended.isSchemaOperation method
        String failingFile = "wrong_schema_statements_runtime.cypher";

        // the failed file is skipped
        testCallEmpty(db, "CALL apoc.cypher.runSchemaFile($file)", Map.of("file", failingFile));

        clearSchema();

        // the failed file produces an "error" row
        testCall(db, "CALL apoc.cypher.runSchemaFile($file, {reportError: true})", Map.of("file", failingFile), row -> {
            String cypherError = "Error in `wrong_schema_statements_runtime.cypher`:\n" + "Variable `bar` not defined";
            assertErrorResult(cypherError, row);
        });
    }

    @Test
    public void testRunSchemaFilesWithFailingStatement() {
        String failingFile = "constraints.cypher";
        String cypherError = "Error in `constraints.cypher`:\n" + "An equivalent constraint already exists";

        testRunFailingSchemaFilesCommon(failingFile, cypherError);
    }

    @Test
    public void testRunSchemaFilesWithFailingExplain() {
        // error during CypherExtended.isSchemaOperation method
        String failingFile = "wrong_schema_statements_runtime.cypher";
        String cypherError = "Error in `wrong_schema_statements_runtime.cypher`:\n" + "Variable `bar` not defined";

        testRunFailingSchemaFilesCommon(failingFile, cypherError);
    }

    private void testRunFailingSchemaFilesCommon(String failingFile, String cypherError) {
        List<String> files = List.of("constraints.cypher", failingFile, "schema.cypher");

        testResult(db, "CALL apoc.cypher.runSchemaFiles($files)", Map.of("files", files), r -> {
            assertConstraintsCypherFile(r);
            assertSchemaCypherFile(r);
            assertFalse(r.hasNext());
        });

        clearSchema();

        // the failed file produces an "error" row
        testResult(db, "CALL apoc.cypher.runSchemaFiles($files, {reportError: true})", Map.of("files", files), r -> {
            assertConstraintsCypherFile(r);

            Map<String, Object> row = r.next();
            assertErrorResult(cypherError, row);

            assertSchemaCypherFile(r);
            assertFalse(r.hasNext());
        });
    }

    private void assertConstraintsCypherFile(Result r) {
        Map<String, Object> row = r.next();
        Map result = (Map) row.get("result");
        assertEquals(1L, toLong(result.get("constraintsAdded")));
        row = r.next();
        result = (Map) row.get("result");
        assertEquals(1L, toLong(result.get("constraintsAdded")));
    }

    @Test
    public void testRunFilesWithFailingStatement() {
        String failingFile = "wrong_statements_runtime.cypher";
        String cypherError = "already exists with label `Fail` and property `foo` = 1";

        testRunFailingFilesCommon(failingFile, cypherError);
    }

    @Test
    public void testRunFilesWithFailingPeriodicStatement() {
        String failingFile = "wrong_call_in_transactions.cypher";
        String cypherError = "already exists with label `Fail` and property `foo` = 1";
        testRunFailingFilesCommon(failingFile, cypherError);
    }

    @Test
    public void testLongRunningRunFilesWithFailingPeriodicStatement() {
        String failingFile = "wrong_call_in_transactions.cypher";
        String cypherError = "already exists with label `Fail` and property `foo` = 1";

        db.executeTransactionally("CREATE CONSTRAINT FOR (n:Fail) REQUIRE n.foo IS UNIQUE");
        db.executeTransactionally("CREATE (n:Fail {foo: 1})");

        List<String> files = List.of("long-create.cypher", failingFile, "long-create.cypher");

        // the failed file is skipped
        testCallEmpty(db, "CALL apoc.cypher.runFiles($files, {statistics: false})", Map.of("files", files));

        // the failed file produces an "error" row
        testCall(
                db,
                "CALL apoc.cypher.runFiles($files, {statistics: false, reportError: true})",
                Map.of("files", files),
                r -> assertErrorResult(cypherError, r));
    }

    @Test
    public void testRunFilesWithFailingExplain() {
        // error during CypherExtended.isSchemaOperation method
        String failingFile = "wrong_statements.cypher";
        String cypherError = "Invalid input ')': expected";
        testRunFailingFilesCommon(failingFile, cypherError);
    }

    private void testRunFailingFilesCommon(String failingFile, String cypherError) {
        db.executeTransactionally("CREATE CONSTRAINT FOR (n:Fail) REQUIRE n.foo IS UNIQUE");
        db.executeTransactionally("CREATE (n:Fail {foo: 1})");

        List<String> files = List.of("create_delete.cypher", failingFile, "create_delete.cypher");

        // the failed file is skipped
        testResult(db, "CALL apoc.cypher.runFiles($files)", Map.of("files", files), r -> {
            assertCreateDeleteFile(r);
            assertCreateDeleteFile(r);
            assertFalse(r.hasNext());
        });

        // the failed file produces an "error" row
        testResult(db, "CALL apoc.cypher.runFiles($files, {reportError: true})", Map.of("files", files), r -> {
            assertCreateDeleteFile(r);

            Map<String, Object> next = r.next();
            assertErrorResult(cypherError, next);
            assertCreateDeleteFile(r);
            assertFalse(r.hasNext());
        });
    }

    @Test
    public void testSchemaRunFile() {
        final int expectedBefore;
        try (Transaction tx = db.beginTx()) {
            expectedBefore = count(tx.schema().getIndexes());
        }

        testResult(db, "CALL apoc.cypher.runSchemaFile('schema.cypher')", r -> {
            assertSchemaCypherFile(r);
            assertFalse(r.hasNext());
        });

        try (Transaction tx = db.beginTx()) {
            assertEquals(expectedBefore + 4, count(tx.schema().getIndexes()));
        }
    }

    private void assertSchemaCypherFile(Result r) {
        Map<String, Object> row = r.next();
        Map result = (Map) row.get("result");
        assertEquals(1L, toLong(result.get("indexesAdded")));
        row = r.next();
        result = (Map) row.get("result");
        assertEquals(1L, toLong(result.get("indexesAdded")));
        row = r.next();
        result = (Map) row.get("result");
        assertEquals(1L, toLong(result.get("indexesAdded")));
        row = r.next();
        result = (Map) row.get("result");
        assertEquals(1L, toLong(result.get("indexesAdded")));
    }

    @Test
    public void testSchemaRunFiles() {
        schemaAssertions(Collections.emptyList(), Collections.emptyList());

        testCallEmpty(
                db,
                "CALL apoc.cypher.runSchemaFiles($files, {statistics: false})",
                map("files", List.of("constraints.cypher", "drop_constraints.cypher", "schema.cypher")));

        final List<String> another_cons =
                List.of("CustomerIndex1", "CustomerIndex21", "CustomerIndex231", "another_cons", "node_index_name");
        final List<String> another_cons1 = List.of("another_cons");

        schemaAssertions(another_cons, another_cons1);
    }

    private void schemaAssertions(List<String> expectedIdx, List<String> expectedCons) {
        try (Transaction tx = db.beginTx()) {
            final Schema schema = tx.schema();
            schema.awaitIndexesOnline(20, TimeUnit.SECONDS);
            final List<String> actualIdx = StreamSupport.stream(
                            schema.getIndexes().spliterator(), false)
                    .filter(idx -> !idx.getIndexType().equals(IndexType.LOOKUP))
                    .map(IndexDefinition::getName)
                    .sorted()
                    .collect(Collectors.toList());
            final List<String> actualCons = StreamSupport.stream(
                            schema.getConstraints().spliterator(), false)
                    .map(ConstraintDefinition::getName)
                    .sorted()
                    .collect(Collectors.toList());
            assertEquals(expectedIdx, actualIdx);
            assertEquals(expectedCons, actualCons);
        }
    }

    @Test
    @Ignore
    public void testSchemaRunMixedSchemaAndDataFile() throws Exception {
        testResult(db, "CALL apoc.cypher.runSchemaFile('schema_create.cypher')", r -> {
            Map<String, Object> row = r.next();
            Map result = (Map) row.get("result");
            assertEquals(1L, toLong(result.get("indexesAdded")));
        });
    }

    @Test
    public void testRunFileWithEmptyFile() throws Exception {
        testResult(db, "CALL apoc.cypher.runFile('empty.cypher')", r -> assertFalse("should be empty", r.hasNext()));
    }

    @Test
    public void lengthyRunManyShouldTerminate() {
        String repetetiveStatement =
                "CALL apoc.cypher.runFile(\"enrollment-incremental.cypher\",{parameters: {SubID: \"218598584\", Account_Number: \"\", AccountType: \"\",Source: \"VerizonMASnapshot\", MDN: \"\", Offering: \"\", Enroll_Date: \"\", Product_SKU: \"\", Device_Model: \"\", Device_Make: \"\", First_Name: \"\", Last_Name: \"\",Email1: \"\", Email2: \"\", Email3: \"\", Postal_CD: \"\", City: \"\", State: \"\", BillingStatus: \"\", ActionType: \"Drop\", Text_Date : \"2020-03-11\"}}) yield result return sum(result.total) as total;\n"
                        + "CALL apoc.cypher.runFile(\"enrollment-incremental.cypher\",{parameters: {SubID: \"7898935\", Account_Number: \"\", AccountType: \"\",Source: \"VerizonNorthSnapshot\", MDN: \"\", Offering: \"\", Enroll_Date: \"\", Product_SKU: \"\", Device_Model: \"\", Device_Make: \"\", First_Name: \"\", Last_Name: \"\",Email1: \"\", Email2: \"\", Email3: \"\", Postal_CD: \"\", City: \"\", State: \"\", BillingStatus: \"\", ActionType: \"Drop\", Text_Date : \"2020-03-11\"}}) yield result return sum(result.total) as total;\n";

        String cypher = String.format(
                "CALL apoc.cypher.runMany('%s',{statistics:true,timeout:60}) yield result return sum(result.total) as total;",
                String.join("", Collections.nCopies(25, repetetiveStatement)));

        testResult(db, cypher, result -> {
            Map<String, Object> single = Iterators.single(result);
            assertEquals(50l, single.get("total"));
        });
    }

    private static void assertErrorResult(String cypherError, Map<String, Object> r) {
        assertEquals(-1L, r.get("row"));
        Map<String, String> result = (Map<String, String>) r.get("result");
        assertThat(result.get("error"), containsString(cypherError));
    }

    private void assertCreateDeleteFile(Result r) {
        Map<String, Object> row = r.next();
        assertEquals(-1L, row.get("row"));
        Map result = (Map) row.get("result");
        assertEquals(1L, toLong(result.get("nodesCreated")));
        assertEquals(1L, toLong(result.get("labelsAdded")));
        assertEquals(1L, toLong(result.get("propertiesSet")));
        row = r.next();
        result = (Map) row.get("result");
        assertEquals(-1L, row.get("row"));
        assertEquals(1L, toLong(result.get("nodesDeleted")));
    }
}
