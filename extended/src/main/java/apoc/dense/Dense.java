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

import apoc.Extended;
import java.util.Map;
import java.util.stream.Stream;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.UserFunction;

/**
 * Procedures for automatically creating and managing multi-level tree substructures
 * for dense (supernode) nodes with millions of relationships.
 *
 * <p>Implements <a href="https://github.com/neo4j-contrib/neo4j-apoc-procedures/issues/614">APOC #614</a>:
 * automatic substructure dense nodes.</p>
 *
 * <h2>Architecture</h2>
 * <p>When a node has millions of relationships of a given type, traversing or writing
 * to that node becomes a bottleneck. This module distributes those relationships across
 * a B-tree of intermediate "bucket" nodes ({@code __DenseBucket} label), reducing both
 * read fan-out and write lock contention.</p>
 *
 * <h2>Relationship Type Convention</h2>
 * <p>All internal relationships use prefixed types to avoid collision with user data:</p>
 * <ul>
 *   <li>{@code _DENSE_META_<TYPE>} — source node to root bucket</li>
 *   <li>{@code _DENSE_BRANCH_<TYPE>} — parent bucket to child bucket</li>
 *   <li>{@code _DENSE_<TYPE>} — leaf bucket to target node</li>
 * </ul>
 *
 * <h2>Required Index</h2>
 * <p>An index on {@code __DenseBucket.__dense_source} is required and will be
 * auto-created on first use.</p>
 *
 * <h2>Direction Semantics</h2>
 * <ul>
 *   <li><strong>Write procedures</strong> (create, delete, migrate, flatten): require
 *       OUTGOING or INCOMING. Direction.BOTH is rejected with an error directing users
 *       to call the procedure twice.</li>
 *   <li><strong>Read procedures</strong> (relationships, degree, status): support BOTH
 *       by returning the union of OUTGOING and INCOMING results.</li>
 * </ul>
 *
 * <h2>Relationship ID Contract</h2>
 * <p><strong>WARNING:</strong> Relationship element IDs are NOT preserved across
 * {@code apoc.dense.migrate()} or {@code apoc.dense.flatten()}. These procedures delete
 * original relationships and create new ones, causing Neo4j to assign new element IDs.
 * Applications must not cache relationship IDs for nodes that have been or may be
 * migrated. Use {@code apoc.dense.status()} to check migration state. This is consistent
 * with the behavior of {@code apoc.refactor.*} procedures.</p>
 *
 * <h2>Element ID Portability</h2>
 * <p><strong>WARNING:</strong> The {@code __dense_target} property on leaf relationships
 * stores the target node's element ID, which is stable within a database lifecycle but
 * NOT portable across dump/restore, database copy, or seed operations. After restoring
 * a database from a dump, element IDs change and all {@code __dense_target} properties
 * become stale. Recovery path: call {@code apoc.dense.flatten()} to restore direct
 * relationships, then {@code apoc.dense.migrate()} to rebuild the substructure with
 * fresh element IDs. This is a known Neo4j ecosystem constraint affecting all
 * element-ID-based indexes.</p>
 *
 * @see DenseNodeManager
 * @see DenseConfig
 */
@Extended
public class Dense {

    @Context
    public Transaction tx;

    @Context
    public GraphDatabaseService db;

    @Context
    public Log log;

    /**
     * Thread-safe flag for one-time index creation. Uses volatile + synchronized
     * with explicit exception handling for concurrent schema operations.
     */
    private static volatile boolean indexEnsured = false;

    private DenseNodeManager manager() {
        if (!indexEnsured) {
            synchronized (Dense.class) {
                if (!indexEnsured) {
                    DenseNodeManager.ensureIndexes(db, log);
                    indexEnsured = true;
                }
            }
        }
        return new DenseNodeManager(tx, log);
    }

    // ========================================================================
    // WRITE PROCEDURES
    // ========================================================================

    @Procedure(name = "apoc.dense.create.relationship", mode = Mode.WRITE)
    @Description("Creates a relationship through the dense node substructure. "
            + "Routes the relationship through a B-tree of bucket nodes to distribute "
            + "write contention. Initializes the substructure if it doesn't exist yet. "
            + "Direction must be OUTGOING or INCOMING (not BOTH). "
            + "YIELD rel, bucket")
    public Stream<DenseResult.CreateRelationshipResult> createRelationship(
            @Name("sourceNode") Node sourceNode,
            @Name("type") String type,
            @Name("targetNode") Node targetNode,
            @Name(value = "props", defaultValue = "{}") Map<String, Object> props,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        DenseConfig denseConfig = new DenseConfig(config);
        Direction direction = Direction.OUTGOING;

        if (denseConfig.isAutoMigrate()) {
            warnIfDirectRelsDense(sourceNode, type, direction, denseConfig);
        }

        DenseResult.CreateRelationshipResult result =
                manager().createRelationship(sourceNode, type, direction, props, targetNode, denseConfig);
        return Stream.of(result);
    }

    @Procedure(name = "apoc.dense.create.relationship.incoming", mode = Mode.WRITE)
    @Description("Creates an incoming relationship through the dense node substructure. " + "YIELD rel, bucket")
    public Stream<DenseResult.CreateRelationshipResult> createRelationshipIncoming(
            @Name("sourceNode") Node sourceNode,
            @Name("type") String type,
            @Name("targetNode") Node targetNode,
            @Name(value = "props", defaultValue = "{}") Map<String, Object> props,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        DenseConfig denseConfig = new DenseConfig(config);
        DenseResult.CreateRelationshipResult result =
                manager().createRelationship(sourceNode, type, Direction.INCOMING, props, targetNode, denseConfig);
        return Stream.of(result);
    }

    @Procedure(name = "apoc.dense.delete.relationship", mode = Mode.WRITE)
    @Description("Deletes a relationship from the dense node substructure. "
            + "Uses indexed lookup via __dense_target for efficient deletion. "
            + "Worst case O(buckets_for_source) when duplicate source-target pairs exist across buckets. "
            + "Compacts empty buckets automatically. "
            + "YIELD removed, remainingCount")
    public Stream<DenseResult.DeleteRelationshipResult> deleteRelationship(
            @Name("sourceNode") Node sourceNode,
            @Name("type") String type,
            @Name("targetNode") Node targetNode,
            @Name(value = "matchProps", defaultValue = "{}") Map<String, Object> matchProps) {

        DenseResult.DeleteRelationshipResult result =
                manager().deleteRelationship(sourceNode, type, Direction.OUTGOING, targetNode, matchProps);
        return Stream.of(result);
    }

    // ========================================================================
    // READ PROCEDURES
    // ========================================================================

    @Procedure(name = "apoc.dense.relationships", mode = Mode.READ)
    @Description("Queries relationships through the dense node substructure. "
            + "Supports cursor-based pagination and Direction.BOTH (returns union). "
            + "YIELD rel, node, cursor")
    public Stream<DenseResult.RelationshipQueryResult> relationships(
            @Name("sourceNode") Node sourceNode,
            @Name("type") String type,
            @Name(value = "direction", defaultValue = "OUTGOING") String directionStr,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        DenseConfig denseConfig = new DenseConfig(config);
        Direction direction = parseDirection(directionStr);

        return manager().queryRelationships(sourceNode, type, direction, denseConfig.getCursor(), denseConfig.getLimit());
    }

    @UserFunction(name = "apoc.dense.degree")
    @Description("Returns the total relationship count for a dense node substructure "
            + "without traversing the tree. Uses metadata for O(1) count. "
            + "Supports Direction.BOTH (returns sum of OUTGOING + INCOMING).")
    public long degree(
            @Name("sourceNode") Node sourceNode,
            @Name("type") String type,
            @Name(value = "direction", defaultValue = "OUTGOING") String directionStr) {

        Direction direction = parseDirection(directionStr);
        return new DenseNodeManager(tx, log).degree(sourceNode, type, direction);
    }

    // ========================================================================
    // ANALYSIS & MIGRATION
    // ========================================================================

    @Procedure(name = "apoc.dense.analyze", mode = Mode.READ)
    @Description("Detects dense nodes exceeding the configured threshold. "
            + "Streams results as found (never buffers full set). "
            + "Supports sampleRate (0.0-1.0) for probabilistic sampling and "
            + "analyzeLimit (default 500) to cap results on large graphs. "
            + "YIELD node, type, direction, degree, alreadyManaged")
    public Stream<DenseResult.AnalyzeResult> analyze(
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        DenseConfig denseConfig = new DenseConfig(config);
        return new DenseNodeManager(tx, log).analyze(denseConfig);
    }

    @Procedure(name = "apoc.dense.migrate", mode = Mode.WRITE)
    @Description("Migrates existing direct relationships into the dense substructure. "
            + "Processes up to batchSize relationships per call. Call repeatedly until "
            + "migrationComplete is true. Sets __dense_migration_in_progress flag during migration. "
            + "WARNING: Relationship element IDs are not preserved — see class javadoc. "
            + "Direction must be OUTGOING or INCOMING (not BOTH). "
            + "YIELD migratedCount, bucketsCreated, levels, migrationComplete")
    public Stream<DenseResult.MigrateResult> migrate(
            @Name("sourceNode") Node sourceNode,
            @Name("type") String type,
            @Name(value = "direction", defaultValue = "OUTGOING") String directionStr,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        DenseConfig denseConfig = new DenseConfig(config);
        Direction direction = parseDirection(directionStr);

        DenseResult.MigrateResult result = manager().migrate(sourceNode, type, direction, denseConfig);
        return Stream.of(result);
    }

    @Procedure(name = "apoc.dense.flatten", mode = Mode.WRITE)
    @Description("Removes the dense substructure and reconnects relationships directly "
            + "to the source node. Reverse of apoc.dense.migrate. Processes up to batchSize "
            + "relationships per call. WARNING: Relationship element IDs are not preserved. "
            + "Direction must be OUTGOING or INCOMING (not BOTH). "
            + "YIELD flattenedCount, bucketsRemoved")
    public Stream<DenseResult.FlattenResult> flatten(
            @Name("sourceNode") Node sourceNode,
            @Name("type") String type,
            @Name(value = "direction", defaultValue = "OUTGOING") String directionStr,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        DenseConfig denseConfig = new DenseConfig(config);
        Direction direction = parseDirection(directionStr);

        DenseResult.FlattenResult result = manager().flatten(sourceNode, type, direction, denseConfig);
        return Stream.of(result);
    }

    @Procedure(name = "apoc.dense.status", mode = Mode.READ)
    @Description("Returns status information about a dense node substructure including "
            + "total count, tree levels, bucket count, and migration state. "
            + "Supports Direction.BOTH (returns both OUTGOING and INCOMING rows). "
            + "YIELD type, direction, totalCount, levels, bucketCount, bucketCapacity, branchFactor, migrationInProgress")
    public Stream<DenseResult.StatusResult> status(
            @Name("sourceNode") Node sourceNode,
            @Name("type") String type,
            @Name(value = "direction", defaultValue = "OUTGOING") String directionStr) {

        Direction direction = parseDirection(directionStr);
        return new DenseNodeManager(tx, log).statusStream(sourceNode, type, direction);
    }

    // ========================================================================
    // PRIVATE HELPERS
    // ========================================================================

    private Direction parseDirection(String directionStr) {
        if (directionStr == null || directionStr.isEmpty()) {
            return Direction.OUTGOING;
        }
        switch (directionStr.toUpperCase()) {
            case "INCOMING":
            case "IN":
                return Direction.INCOMING;
            case "BOTH":
                return Direction.BOTH;
            case "OUTGOING":
            case "OUT":
            default:
                return Direction.OUTGOING;
        }
    }

    /**
     * Emits a log warning if the source node already has direct relationships exceeding the
     * dense threshold. This is the behavior when autoMigrate is true — it warns via the
     * Neo4j procedure log (visible in neo4j.log) rather than silently ignoring or
     * automatically migrating, letting operators decide when to run apoc.dense.migrate().
     */
    private void warnIfDirectRelsDense(Node sourceNode, String type, Direction direction, DenseConfig config) {
        int directCount = 0;
        for (var ignored :
                sourceNode.getRelationships(direction, org.neo4j.graphdb.RelationshipType.withName(type))) {
            directCount++;
            if (directCount >= config.getDenseThreshold()) {
                log.warn(
                        "Node %s has %d+ direct '%s' relationships exceeding dense threshold %d. "
                                + "Consider running apoc.dense.migrate() first.",
                        sourceNode.getElementId(), directCount, type, config.getDenseThreshold());
                break;
            }
        }
    }
}
