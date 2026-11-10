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
package apoc.dense;

import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static org.junit.Assert.*;

import apoc.util.TestUtil;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.*;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

/**
 * Integration tests for apoc.dense.* procedures.
 * Tests follow APOC conventions using ImpermanentDbmsRule and TestUtil.
 */
public class DenseTest {

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();

    @Before
    public void setUp() throws Exception {
        TestUtil.registerProcedure(db, Dense.class);
    }

    @After
    public void teardown() {
        db.shutdown();
    }

    // ========================================================================
    // CREATE RELATIONSHIP TESTS
    // ========================================================================

    @Test
    public void testCreateSingleRelationship() {
        db.executeTransactionally("CREATE (:Source {name:'s'}), (:Target {name:'t'})");

        testCall(
                db,
                "MATCH (s:Source), (t:Target) "
                        + "CALL apoc.dense.create.relationship(s, 'LIKES', t) YIELD rel, bucket "
                        + "RETURN rel, bucket",
                (row) -> {
                    Relationship rel = (Relationship) row.get("rel");
                    Node bucket = (Node) row.get("bucket");
                    assertNotNull(rel);
                    assertNotNull(bucket);
                    assertTrue(bucket.hasLabel(Label.label("__DenseBucket")));
                    assertEquals("_DENSE_LIKES", rel.getType().name());
                });
    }

    @Test
    public void testCreateRelationshipWithProperties() {
        db.executeTransactionally("CREATE (:Source {name:'s'}), (:Target {name:'t'})");

        testCall(
                db,
                "MATCH (s:Source), (t:Target) "
                        + "CALL apoc.dense.create.relationship(s, 'LIKES', t, {since: 2024, weight: 0.9}) YIELD rel "
                        + "RETURN rel",
                (row) -> {
                    Relationship rel = (Relationship) row.get("rel");
                    assertEquals(2024L, rel.getProperty("since"));
                    assertEquals(0.9, (double) rel.getProperty("weight"), 0.001);
                });
    }

    @Test
    public void testCreateMultipleRelationshipsFillsBucket() {
        db.executeTransactionally("CREATE (:Source {name:'s'})");
        db.executeTransactionally("UNWIND range(1, 10) AS i CREATE (:Target {idx: i})");

        testResult(
                db,
                "MATCH (s:Source), (t:Target) "
                        + "WITH s, t ORDER BY t.idx "
                        + "CALL apoc.dense.create.relationship(s, 'LIKES', t) YIELD rel, bucket "
                        + "RETURN count(rel) AS relCount, count(DISTINCT bucket) AS bucketCount",
                (result) -> {
                    Map<String, Object> row = result.next();
                    assertEquals(10L, row.get("relCount"));
                    assertEquals(1L, row.get("bucketCount"));
                });
    }

    @Test
    public void testCreateRelationshipsBucketSplit() {
        db.executeTransactionally("CREATE (:Source {name:'s'})");
        db.executeTransactionally("UNWIND range(1, 15) AS i CREATE (:Target {idx: i})");

        testResult(
                db,
                "MATCH (s:Source), (t:Target) "
                        + "WITH s, t ORDER BY t.idx "
                        + "CALL apoc.dense.create.relationship(s, 'LIKES', t, {}, {bucketCapacity: 5}) YIELD rel, bucket "
                        + "RETURN count(rel) AS relCount, count(DISTINCT bucket) AS bucketCount",
                (result) -> {
                    Map<String, Object> row = result.next();
                    assertEquals(15L, row.get("relCount"));
                    long bucketCount = (long) row.get("bucketCount");
                    assertTrue("Expected at least 3 buckets, got " + bucketCount, bucketCount >= 3);
                });
    }

    @Test
    public void testCreateRelationshipsMultipleLevels() {
        db.executeTransactionally("CREATE (:Source {name:'s'})");
        db.executeTransactionally("UNWIND range(1, 50) AS i CREATE (:Target {idx: i})");

        testResult(
                db,
                "MATCH (s:Source), (t:Target) "
                        + "WITH s, t ORDER BY t.idx "
                        + "CALL apoc.dense.create.relationship(s, 'LIKES', t, {}, "
                        + "  {bucketCapacity: 5, branchFactor: 3}) YIELD rel "
                        + "RETURN count(rel) AS cnt",
                (result) -> {
                    Map<String, Object> row = result.next();
                    assertEquals(50L, row.get("cnt"));
                });

        testCall(
                db,
                "MATCH (s:Source) CALL apoc.dense.status(s, 'LIKES') YIELD levels, totalCount "
                        + "RETURN levels, totalCount",
                (row) -> {
                    long totalCount = (long) row.get("totalCount");
                    int levels = ((Number) row.get("levels")).intValue();
                    assertEquals(50L, totalCount);
                    assertTrue("Expected multiple levels, got " + levels, levels >= 2);
                });
    }

    // ========================================================================
    // DEGREE FUNCTION TESTS
    // ========================================================================

    @Test
    public void testDegreeFunction() {
        db.executeTransactionally("CREATE (:Source {name:'s'})");
        db.executeTransactionally("UNWIND range(1, 25) AS i CREATE (:Target {idx: i})");

        db.executeTransactionally(
                "MATCH (s:Source), (t:Target) "
                        + "WITH s, t ORDER BY t.idx "
                        + "CALL apoc.dense.create.relationship(s, 'LIKES', t) YIELD rel "
                        + "RETURN count(rel)");

        testCall(
                db,
                "MATCH (s:Source) RETURN apoc.dense.degree(s, 'LIKES') AS deg",
                (row) -> assertEquals(25L, row.get("deg")));
    }

    @Test
    public void testDegreeReturnsZeroForNonDenseNode() {
        db.executeTransactionally("CREATE (:Source {name:'s'})");

        testCall(
                db,
                "MATCH (s:Source) RETURN apoc.dense.degree(s, 'LIKES') AS deg",
                (row) -> assertEquals(0L, row.get("deg")));
    }

    // ========================================================================
    // QUERY RELATIONSHIPS TESTS
    // ========================================================================

    @Test
    public void testQueryRelationships() {
        db.executeTransactionally("CREATE (:Source {name:'s'})");
        db.executeTransactionally("UNWIND range(1, 10) AS i CREATE (:Target {idx: i})");

        db.executeTransactionally(
                "MATCH (s:Source), (t:Target) "
                        + "WITH s, t ORDER BY t.idx "
                        + "CALL apoc.dense.create.relationship(s, 'LIKES', t, {weight: t.idx}) YIELD rel "
                        + "RETURN count(rel)");

        testResult(
                db,
                "MATCH (s:Source) "
                        + "CALL apoc.dense.relationships(s, 'LIKES') YIELD rel, node, cursor "
                        + "RETURN rel, node, cursor",
                (result) -> {
                    int count = 0;
                    while (result.hasNext()) {
                        Map<String, Object> row = result.next();
                        assertNotNull(row.get("rel"));
                        assertNotNull(row.get("node"));
                        assertNotNull(row.get("cursor"));
                        count++;
                    }
                    assertEquals(10, count);
                });
    }

    @Test
    public void testQueryRelationshipsWithLimit() {
        db.executeTransactionally("CREATE (:Source {name:'s'})");
        db.executeTransactionally("UNWIND range(1, 20) AS i CREATE (:Target {idx: i})");

        db.executeTransactionally(
                "MATCH (s:Source), (t:Target) "
                        + "WITH s, t ORDER BY t.idx "
                        + "CALL apoc.dense.create.relationship(s, 'LIKES', t) YIELD rel "
                        + "RETURN count(rel)");

        testResult(
                db,
                "MATCH (s:Source) "
                        + "CALL apoc.dense.relationships(s, 'LIKES', 'OUTGOING', {limit: 5}) YIELD node "
                        + "RETURN count(node) AS cnt",
                (result) -> {
                    Map<String, Object> row = result.next();
                    assertEquals(5L, row.get("cnt"));
                });
    }

    @Test
    public void testQueryRelationshipsCursorPagination() {
        db.executeTransactionally("CREATE (:Source {name:'s'})");
        db.executeTransactionally("UNWIND range(1, 10) AS i CREATE (:Target {idx: i})");

        db.executeTransactionally(
                "MATCH (s:Source), (t:Target) "
                        + "WITH s, t ORDER BY t.idx "
                        + "CALL apoc.dense.create.relationship(s, 'LIKES', t) YIELD rel "
                        + "RETURN count(rel)");

        // Get first page (5 results)
        final String[] savedCursor = new String[1];
        testResult(
                db,
                "MATCH (s:Source) "
                        + "CALL apoc.dense.relationships(s, 'LIKES', 'OUTGOING', {limit: 5}) YIELD cursor "
                        + "RETURN cursor",
                (result) -> {
                    String lastCursor = null;
                    int count = 0;
                    while (result.hasNext()) {
                        lastCursor = (String) result.next().get("cursor");
                        count++;
                    }
                    assertEquals(5, count);
                    savedCursor[0] = lastCursor;
                });

        // Get second page using cursor
        testResult(
                db,
                "MATCH (s:Source) "
                        + "CALL apoc.dense.relationships(s, 'LIKES', 'OUTGOING', {limit: 5, cursor: $cursor}) YIELD node "
                        + "RETURN count(node) AS cnt",
                Map.of("cursor", savedCursor[0]),
                (result) -> {
                    Map<String, Object> row = result.next();
                    assertEquals(5L, row.get("cnt"));
                });
    }

    // ========================================================================
    // DELETE RELATIONSHIP TESTS
    // ========================================================================

    @Test
    public void testDeleteRelationship() {
        db.executeTransactionally("CREATE (:Source {name:'s'}), (:Target {name:'t'})");

        db.executeTransactionally(
                "MATCH (s:Source), (t:Target) "
                        + "CALL apoc.dense.create.relationship(s, 'LIKES', t, {weight: 1}) YIELD rel "
                        + "RETURN rel");

        testCall(
                db,
                "MATCH (s:Source), (t:Target) "
                        + "CALL apoc.dense.delete.relationship(s, 'LIKES', t) YIELD removed, remainingCount "
                        + "RETURN removed, remainingCount",
                (row) -> {
                    assertTrue((boolean) row.get("removed"));
                    assertEquals(0L, row.get("remainingCount"));
                });

        testCall(
                db,
                "MATCH (s:Source) RETURN apoc.dense.degree(s, 'LIKES') AS deg",
                (row) -> assertEquals(0L, row.get("deg")));
    }

    @Test
    public void testDeleteRelationshipWithPropertyMatching() {
        db.executeTransactionally("CREATE (:Source {name:'s'}), (:Target {name:'t'})");

        db.executeTransactionally(
                "MATCH (s:Source), (t:Target) "
                        + "CALL apoc.dense.create.relationship(s, 'LIKES', t, {weight: 1}) YIELD rel "
                        + "RETURN rel");
        db.executeTransactionally(
                "MATCH (s:Source), (t:Target) "
                        + "CALL apoc.dense.create.relationship(s, 'LIKES', t, {weight: 2}) YIELD rel "
                        + "RETURN rel");

        testCall(
                db,
                "MATCH (s:Source), (t:Target) "
                        + "CALL apoc.dense.delete.relationship(s, 'LIKES', t, {weight: 1}) "
                        + "YIELD removed, remainingCount "
                        + "RETURN removed, remainingCount",
                (row) -> {
                    assertTrue((boolean) row.get("removed"));
                    assertEquals(1L, row.get("remainingCount"));
                });
    }

    @Test
    public void testDeleteNonExistentRelationship() {
        db.executeTransactionally("CREATE (:Source {name:'s'}), (:Target {name:'t'})");

        testCall(
                db,
                "MATCH (s:Source), (t:Target) "
                        + "CALL apoc.dense.delete.relationship(s, 'LIKES', t) YIELD removed "
                        + "RETURN removed",
                (row) -> assertFalse((boolean) row.get("removed")));
    }

    @Test
    public void testDeleteCompactsEmptyBuckets() {
        db.executeTransactionally("CREATE (:Source {name:'s'})");
        db.executeTransactionally("UNWIND range(1, 6) AS i CREATE (:Target {idx: i})");

        db.executeTransactionally(
                "MATCH (s:Source), (t:Target) "
                        + "WITH s, t ORDER BY t.idx "
                        + "CALL apoc.dense.create.relationship(s, 'LIKES', t, {}, {bucketCapacity: 3}) YIELD rel "
                        + "RETURN count(rel)");

        db.executeTransactionally(
                "MATCH (s:Source), (t:Target) "
                        + "WITH s, t ORDER BY t.idx "
                        + "CALL apoc.dense.delete.relationship(s, 'LIKES', t) YIELD removed "
                        + "RETURN count(removed)");

        try (Transaction tx = db.beginTx()) {
            ResourceIterator<Node> buckets = tx.findNodes(Label.label("__DenseBucket"));
            assertFalse("Expected no remaining bucket nodes", buckets.hasNext());
        }
    }

    // ========================================================================
    // ANALYZE TESTS
    // ========================================================================

    @Test
    public void testAnalyzeDetectsDenseNodes() {
        db.executeTransactionally("CREATE (s:Source {name:'s'})");
        db.executeTransactionally("UNWIND range(1, 20) AS i CREATE (:Target {idx: i})");
        db.executeTransactionally("MATCH (s:Source), (t:Target) CREATE (s)-[:FOLLOWS]->(t)");

        testResult(
                db,
                "CALL apoc.dense.analyze({denseThreshold: 10}) YIELD node, type, degree, alreadyManaged "
                        + "RETURN node, type, degree, alreadyManaged",
                (result) -> {
                    assertTrue(result.hasNext());
                    Map<String, Object> row = result.next();
                    assertEquals("FOLLOWS", row.get("type"));
                    assertEquals(20L, row.get("degree"));
                    assertFalse((boolean) row.get("alreadyManaged"));
                });
    }

    @Test
    public void testAnalyzeWithLabelFilter() {
        db.executeTransactionally("CREATE (s:Source {name:'s'}), (o:Other {name:'o'})");
        db.executeTransactionally("UNWIND range(1, 20) AS i CREATE (:Target {idx: i})");
        db.executeTransactionally("MATCH (s:Source), (t:Target) CREATE (s)-[:FOLLOWS]->(t)");
        db.executeTransactionally("MATCH (o:Other), (t:Target) CREATE (o)-[:FOLLOWS]->(t)");

        testResult(
                db,
                "CALL apoc.dense.analyze({denseThreshold: 10, label: 'Source'}) " + "YIELD node, type RETURN node, type",
                (result) -> {
                    int count = 0;
                    while (result.hasNext()) {
                        result.next();
                        count++;
                    }
                    assertEquals(1, count);
                });
    }

    // ========================================================================
    // MIGRATE TESTS
    // ========================================================================

    @Test
    public void testMigrateExistingRelationships() {
        db.executeTransactionally("CREATE (s:Source {name:'s'})");
        db.executeTransactionally("UNWIND range(1, 10) AS i CREATE (:Target {idx: i})");
        db.executeTransactionally("MATCH (s:Source), (t:Target) CREATE (s)-[:LIKES {weight: t.idx}]->(t)");

        testCall(
                db,
                "MATCH (s:Source) "
                        + "CALL apoc.dense.migrate(s, 'LIKES', 'OUTGOING', {batchSize: 100}) "
                        + "YIELD migratedCount, migrationComplete "
                        + "RETURN migratedCount, migrationComplete",
                (row) -> {
                    assertEquals(10L, row.get("migratedCount"));
                    assertTrue((boolean) row.get("migrationComplete"));
                });

        // No more direct LIKES relationships
        try (Transaction tx = db.beginTx()) {
            Node source = tx.findNodes(Label.label("Source")).next();
            int directCount = 0;
            for (var ignored : source.getRelationships(Direction.OUTGOING, RelationshipType.withName("LIKES"))) {
                directCount++;
            }
            assertEquals(0, directCount);
        }

        // All accessible via dense query
        testCall(
                db,
                "MATCH (s:Source) RETURN apoc.dense.degree(s, 'LIKES') AS deg",
                (row) -> assertEquals(10L, row.get("deg")));

        // Properties preserved
        testResult(
                db,
                "MATCH (s:Source) "
                        + "CALL apoc.dense.relationships(s, 'LIKES') YIELD rel "
                        + "RETURN rel.weight AS weight ORDER BY weight",
                (result) -> {
                    for (int i = 1; i <= 10; i++) {
                        assertTrue(result.hasNext());
                        Map<String, Object> row = result.next();
                        assertNotNull(row.get("weight"));
                    }
                });
    }

    @Test
    public void testMigrateBatched() {
        db.executeTransactionally("CREATE (s:Source {name:'s'})");
        db.executeTransactionally("UNWIND range(1, 20) AS i CREATE (:Target {idx: i})");
        db.executeTransactionally("MATCH (s:Source), (t:Target) CREATE (s)-[:LIKES]->(t)");

        testCall(
                db,
                "MATCH (s:Source) "
                        + "CALL apoc.dense.migrate(s, 'LIKES', 'OUTGOING', {batchSize: 10}) "
                        + "YIELD migratedCount, migrationComplete "
                        + "RETURN migratedCount, migrationComplete",
                (row) -> {
                    assertEquals(10L, row.get("migratedCount"));
                    assertFalse((boolean) row.get("migrationComplete"));
                });

        testCall(
                db,
                "MATCH (s:Source) "
                        + "CALL apoc.dense.migrate(s, 'LIKES', 'OUTGOING', {batchSize: 10}) "
                        + "YIELD migratedCount, migrationComplete "
                        + "RETURN migratedCount, migrationComplete",
                (row) -> {
                    assertEquals(10L, row.get("migratedCount"));
                    assertTrue((boolean) row.get("migrationComplete"));
                });
    }

    // ========================================================================
    // FLATTEN TESTS
    // ========================================================================

    @Test
    public void testFlattenReverseMigration() {
        db.executeTransactionally("CREATE (:Source {name:'s'})");
        db.executeTransactionally("UNWIND range(1, 10) AS i CREATE (:Target {idx: i})");

        db.executeTransactionally(
                "MATCH (s:Source), (t:Target) "
                        + "WITH s, t ORDER BY t.idx "
                        + "CALL apoc.dense.create.relationship(s, 'LIKES', t, {weight: t.idx}) YIELD rel "
                        + "RETURN count(rel)");

        testCall(
                db,
                "MATCH (s:Source) "
                        + "CALL apoc.dense.flatten(s, 'LIKES', 'OUTGOING', {batchSize: 100}) "
                        + "YIELD flattenedCount, bucketsRemoved "
                        + "RETURN flattenedCount, bucketsRemoved",
                (row) -> {
                    assertEquals(10L, row.get("flattenedCount"));
                    assertTrue((long) row.get("bucketsRemoved") > 0);
                });

        try (Transaction tx = db.beginTx()) {
            Node source = tx.findNodes(Label.label("Source")).next();
            int directCount = 0;
            for (var rel : source.getRelationships(Direction.OUTGOING, RelationshipType.withName("LIKES"))) {
                directCount++;
                assertTrue(rel.hasProperty("weight"));
            }
            assertEquals(10, directCount);
        }

        testCall(
                db,
                "MATCH (s:Source) RETURN apoc.dense.degree(s, 'LIKES') AS deg",
                (row) -> assertEquals(0L, row.get("deg")));
    }

    // ========================================================================
    // STATUS TESTS
    // ========================================================================

    @Test
    public void testStatus() {
        db.executeTransactionally("CREATE (:Source {name:'s'})");
        db.executeTransactionally("UNWIND range(1, 10) AS i CREATE (:Target {idx: i})");
        db.executeTransactionally(
                "MATCH (s:Source), (t:Target) "
                        + "WITH s, t ORDER BY t.idx "
                        + "CALL apoc.dense.create.relationship(s, 'LIKES', t) YIELD rel "
                        + "RETURN count(rel)");

        testCall(
                db,
                "MATCH (s:Source) CALL apoc.dense.status(s, 'LIKES') "
                        + "YIELD type, direction, totalCount, levels, bucketCount, migrationInProgress "
                        + "RETURN type, direction, totalCount, levels, bucketCount, migrationInProgress",
                (row) -> {
                    assertEquals("LIKES", row.get("type"));
                    assertEquals("OUTGOING", row.get("direction"));
                    assertEquals(10L, row.get("totalCount"));
                    assertFalse((boolean) row.get("migrationInProgress"));
                });
    }

    @Test
    public void testStatusEmptyForNonDense() {
        db.executeTransactionally("CREATE (:Source {name:'s'})");

        testResult(
                db,
                "MATCH (s:Source) CALL apoc.dense.status(s, 'LIKES') " + "YIELD type RETURN type",
                (result) -> assertFalse("Expected no results for non-dense node", result.hasNext()));
    }

    // ========================================================================
    // MULTIPLE TYPES / DIRECTIONS TESTS
    // ========================================================================

    @Test
    public void testMultipleRelationshipTypes() {
        db.executeTransactionally("CREATE (:Source {name:'s'})");
        db.executeTransactionally("UNWIND range(1, 5) AS i CREATE (:Target {idx: i})");

        db.executeTransactionally(
                "MATCH (s:Source), (t:Target) "
                        + "WITH s, t ORDER BY t.idx "
                        + "CALL apoc.dense.create.relationship(s, 'LIKES', t) YIELD rel "
                        + "RETURN count(rel)");
        db.executeTransactionally(
                "MATCH (s:Source), (t:Target) "
                        + "WITH s, t ORDER BY t.idx "
                        + "CALL apoc.dense.create.relationship(s, 'FOLLOWS', t) YIELD rel "
                        + "RETURN count(rel)");

        testCall(
                db,
                "MATCH (s:Source) RETURN apoc.dense.degree(s, 'LIKES') AS likes, "
                        + "apoc.dense.degree(s, 'FOLLOWS') AS follows",
                (row) -> {
                    assertEquals(5L, row.get("likes"));
                    assertEquals(5L, row.get("follows"));
                });
    }

    @Test
    public void testIncomingRelationships() {
        db.executeTransactionally("CREATE (:Source {name:'s'}), (:Target {name:'t'})");

        testCall(
                db,
                "MATCH (s:Source), (t:Target) "
                        + "CALL apoc.dense.create.relationship.incoming(s, 'LIKED_BY', t) YIELD rel "
                        + "RETURN rel",
                (row) -> {
                    Relationship rel = (Relationship) row.get("rel");
                    assertNotNull(rel);
                });
    }

    // ========================================================================
    // EDGE CASE TESTS
    // ========================================================================

    @Test
    public void testEmptyPropertiesMap() {
        db.executeTransactionally("CREATE (:Source {name:'s'}), (:Target {name:'t'})");

        testCall(
                db,
                "MATCH (s:Source), (t:Target) "
                        + "CALL apoc.dense.create.relationship(s, 'LIKES', t, {}) YIELD rel "
                        + "RETURN rel",
                (row) -> assertNotNull(row.get("rel")));
    }

    @Test
    public void testBucketNodesAreFilteredFromAnalyze() {
        db.executeTransactionally("CREATE (:Source {name:'s'})");
        db.executeTransactionally("UNWIND range(1, 20) AS i CREATE (:Target {idx: i})");

        db.executeTransactionally(
                "MATCH (s:Source), (t:Target) "
                        + "WITH s, t ORDER BY t.idx "
                        + "CALL apoc.dense.create.relationship(s, 'LIKES', t) YIELD rel "
                        + "RETURN count(rel)");

        testResult(
                db,
                "CALL apoc.dense.analyze({denseThreshold: 5}) YIELD node, type "
                        + "RETURN [l IN labels(node) | l] AS labels, type",
                (result) -> {
                    while (result.hasNext()) {
                        Map<String, Object> row = result.next();
                        @SuppressWarnings("unchecked")
                        List<String> labels = (List<String>) row.get("labels");
                        assertFalse(
                                "Bucket nodes should not appear in analyze results",
                                labels.contains("__DenseBucket"));
                    }
                });
    }

    // ========================================================================
    // INDEXED DELETE TESTS (__dense_target property)
    // ========================================================================

    @Test
    public void testLeafRelationshipsHaveDenseTargetProperty() {
        db.executeTransactionally("CREATE (:Source {name:'s'}), (:Target {name:'t'})");

        db.executeTransactionally(
                "MATCH (s:Source), (t:Target) "
                        + "CALL apoc.dense.create.relationship(s, 'LIKES', t) YIELD rel "
                        + "RETURN rel");

        try (Transaction tx = db.beginTx()) {
            Node target = tx.findNodes(Label.label("Target")).next();
            ResourceIterator<Node> buckets = tx.findNodes(Label.label("__DenseBucket"));
            assertTrue(buckets.hasNext());
            Node bucket = buckets.next();
            for (Relationship rel :
                    bucket.getRelationships(Direction.OUTGOING, RelationshipType.withName("_DENSE_LIKES"))) {
                assertTrue("Leaf rel should have __dense_target property", rel.hasProperty("__dense_target"));
                assertEquals(target.getElementId(), rel.getProperty("__dense_target"));
            }
        }
    }

    @Test
    public void testDeleteUsesIndexedLookup() {
        db.executeTransactionally("CREATE (:Source {name:'s'})");
        db.executeTransactionally("UNWIND range(1, 12) AS i CREATE (:Target {idx: i})");

        db.executeTransactionally(
                "MATCH (s:Source), (t:Target) "
                        + "WITH s, t ORDER BY t.idx "
                        + "CALL apoc.dense.create.relationship(s, 'LIKES', t, {}, {bucketCapacity: 3}) YIELD rel "
                        + "RETURN count(rel)");

        testCall(
                db,
                "MATCH (s:Source), (t:Target {idx: 7}) "
                        + "CALL apoc.dense.delete.relationship(s, 'LIKES', t) YIELD removed, remainingCount "
                        + "RETURN removed, remainingCount",
                (row) -> {
                    assertTrue((boolean) row.get("removed"));
                    assertEquals(11L, row.get("remainingCount"));
                });
    }

    @Test
    public void testFlattenStripsInternalProperties() {
        db.executeTransactionally("CREATE (:Source {name:'s'}), (:Target {name:'t'})");

        db.executeTransactionally(
                "MATCH (s:Source), (t:Target) "
                        + "CALL apoc.dense.create.relationship(s, 'LIKES', t, {weight: 42}) YIELD rel "
                        + "RETURN rel");

        db.executeTransactionally(
                "MATCH (s:Source) "
                        + "CALL apoc.dense.flatten(s, 'LIKES', 'OUTGOING', {batchSize: 100}) "
                        + "YIELD flattenedCount RETURN flattenedCount");

        try (Transaction tx = db.beginTx()) {
            Node source = tx.findNodes(Label.label("Source")).next();
            for (Relationship rel :
                    source.getRelationships(Direction.OUTGOING, RelationshipType.withName("LIKES"))) {
                assertTrue("User property should be preserved", rel.hasProperty("weight"));
                assertFalse(
                        "Internal __dense_target should NOT be on flattened rels",
                        rel.hasProperty("__dense_target"));
            }
        }
    }

    // ========================================================================
    // DIRECTION.BOTH TESTS
    // ========================================================================

    @Test
    public void testMigrateRejectsBothDirection() {
        db.executeTransactionally("CREATE (:Source {name:'s'})");
        try {
            db.executeTransactionally(
                    "MATCH (s:Source) "
                            + "CALL apoc.dense.migrate(s, 'LIKES', 'BOTH') YIELD migratedCount "
                            + "RETURN migratedCount");
            fail("Should have thrown IllegalArgumentException for BOTH direction");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("BOTH") || e.getCause().getMessage().contains("BOTH"));
        }
    }

    @Test
    public void testDegreeWithBothDirection() {
        db.executeTransactionally("CREATE (:Source {name:'s'})");
        db.executeTransactionally("UNWIND range(1, 5) AS i CREATE (:Target {idx: i})");

        db.executeTransactionally(
                "MATCH (s:Source), (t:Target) "
                        + "WITH s, t ORDER BY t.idx "
                        + "CALL apoc.dense.create.relationship(s, 'LIKES', t) YIELD rel "
                        + "RETURN count(rel)");

        db.executeTransactionally(
                "MATCH (s:Source), (t:Target) "
                        + "WITH s, t ORDER BY t.idx "
                        + "CALL apoc.dense.create.relationship.incoming(s, 'LIKES', t) YIELD rel "
                        + "RETURN count(rel)");

        testCall(
                db,
                "MATCH (s:Source) RETURN apoc.dense.degree(s, 'LIKES', 'BOTH') AS deg",
                (row) -> assertEquals(10L, row.get("deg")));
    }

    // ========================================================================
    // ANALYZE WITH LIMIT
    // ========================================================================

    @Test
    public void testAnalyzeWithLimit() {
        db.executeTransactionally("UNWIND range(1, 10) AS i CREATE (:Source {idx: i})");
        db.executeTransactionally("UNWIND range(1, 20) AS i CREATE (:Target {idx: i})");
        db.executeTransactionally("MATCH (s:Source), (t:Target) CREATE (s)-[:FOLLOWS]->(t)");

        testResult(
                db,
                "CALL apoc.dense.analyze({denseThreshold: 10, analyzeLimit: 3}) " + "YIELD node RETURN node",
                (result) -> {
                    int count = 0;
                    while (result.hasNext()) {
                        result.next();
                        count++;
                    }
                    assertTrue("Expected at most 3 results, got " + count, count <= 3);
                });
    }

    // ========================================================================
    // CONCURRENT WRITE TEST
    // ========================================================================

    @Test
    public void testConcurrentCreatesDoNotCorrupt() throws Exception {
        db.executeTransactionally("CREATE (:Source {name:'s'})");
        db.executeTransactionally("UNWIND range(1, 100) AS i CREATE (:Target {idx: i})");

        int threadCount = 4;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        AtomicInteger errors = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(threadCount);

        for (int t = 0; t < threadCount; t++) {
            final int startIdx = t * 25 + 1;
            final int endIdx = startIdx + 24;
            executor.submit(() -> {
                try {
                    for (int i = startIdx; i <= endIdx; i++) {
                        try {
                            db.executeTransactionally(
                                    "MATCH (s:Source), (t:Target {idx: $idx}) "
                                            + "CALL apoc.dense.create.relationship(s, 'LIKES', t, {}, "
                                            + "  {bucketCapacity: 10}) YIELD rel "
                                            + "RETURN rel",
                                    Map.of("idx", (long) i));
                        } catch (Exception e) {
                            if (!e.getMessage().contains("Deadlock")
                                    && !e.getMessage().contains("deadlock")) {
                                errors.incrementAndGet();
                            }
                        }
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await(30, TimeUnit.SECONDS);
        executor.shutdown();

        assertEquals("Unexpected (non-deadlock) errors during concurrent writes", 0, errors.get());

        // Verify data integrity: metadata count matches actual
        testCall(
                db,
                "MATCH (s:Source) "
                        + "WITH s, apoc.dense.degree(s, 'LIKES') AS reported "
                        + "CALL apoc.dense.relationships(s, 'LIKES') YIELD rel "
                        + "WITH reported, count(rel) AS actual "
                        + "RETURN reported, actual",
                (row) -> {
                    long reported = (long) row.get("reported");
                    long actual = (long) row.get("actual");
                    assertEquals("Metadata count should match actual relationship count", reported, actual);
                });
    }
}
