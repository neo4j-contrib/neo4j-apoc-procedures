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

import static apoc.dense.DenseConstants.*;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;

/**
 * Core tree management logic for the dense node substructure.
 *
 * <h2>Tree Structure</h2>
 * <pre>
 *     [Dense Node]
 *          |
 *     {_DENSE_META_LIKES}  (one per type+direction)
 *          |
 *     [Root Bucket]  (__DenseBucket)
 *        / | \
 *   {_DENSE_BRANCH_LIKES}
 *      /   |   \
 *  [Bucket] [Bucket] ...
 *    /|\      /|\
 * {_DENSE_LIKES}  (each carries __dense_target for indexed lookup)
 *  / | \
 * [target nodes]
 * </pre>
 *
 * <h2>Locking Strategy (Deadlock-Safe)</h2>
 * Uses double-checked optimistic locking with strict resource ordering:
 * <ol>
 *   <li>Optimistic read of active bucket pointer (no lock)</li>
 *   <li>Lock the active bucket, re-check count</li>
 *   <li>If capacity available: write (fast path)</li>
 *   <li>If full: release bucket, acquire meta-rel lock FIRST, then re-acquire in
 *       strict topological order (root -&gt; branch -&gt; leaf) to prevent deadlock</li>
 * </ol>
 *
 * <h2>Delete Lookup</h2>
 * <p>Each leaf relationship stores {@code __dense_target} (the target node's element ID)
 * as a property. Combined with the {@code __DenseBucket.__dense_source} index, this
 * enables efficient delete lookup. Complexity is O(buckets_for_source) in the worst
 * case (when the same source-target pair has duplicate relationships across multiple
 * buckets), which is effectively O(log n) for typical unique-target workloads.</p>
 *
 * <h2>Element ID Portability</h2>
 * <p>The {@code __dense_target} property uses Neo4j element IDs which are stable within
 * a database lifecycle but NOT portable across dump/restore or database copy operations.
 * After a restore, run {@code apoc.dense.flatten()} then {@code apoc.dense.migrate()}
 * to rebuild with fresh element IDs.</p>
 */
public class DenseNodeManager {

    private final Transaction tx;
    private final Log log;

    public DenseNodeManager(Transaction tx, Log log) {
        this.tx = tx;
        this.log = log;
    }

    // ========================================================================
    // INDEX MANAGEMENT
    // ========================================================================

    /**
     * Ensures the required indexes exist. Safe to call concurrently — catches
     * "equivalent index already exists" exceptions gracefully.
     *
     * <p>Uses its own transaction for DDL (schema operations cannot be nested
     * inside DML transactions). If the index already exists (concurrent creation
     * or prior session), the exception is caught and ignored.</p>
     */
    public static void ensureIndexes(GraphDatabaseService db, Log log) {
        // Step 1: Read-only check — avoid unnecessary schema tx
        boolean sourceIndexExists = false;
        try (Transaction readTx = db.beginTx()) {
            for (var idx : readTx.schema().getIndexes(BUCKET_LABEL)) {
                for (String prop : idx.getPropertyKeys()) {
                    if (PROP_DENSE_SOURCE.equals(prop)) {
                        sourceIndexExists = true;
                        break;
                    }
                }
                if (sourceIndexExists) break;
            }
        }

        // Step 2: Create missing indexes in separate committed schema transactions
        if (!sourceIndexExists) {
            try (Transaction schemaTx = db.beginTx()) {
                schemaTx.schema()
                        .indexFor(BUCKET_LABEL)
                        .on(PROP_DENSE_SOURCE)
                        .withName(SOURCE_INDEX_NAME)
                        .create();
                schemaTx.commit();
            } catch (Exception e) {
                // EquivalentSchemaRuleAlreadyExistsException or ConstraintViolationException
                // from concurrent creation — safe to ignore
                if (log != null) {
                    log.info(
                            "Dense bucket source index already exists or was created concurrently: "
                                    + e.getMessage());
                }
            }
        }
    }

    // ========================================================================
    // DIRECTION VALIDATION
    // ========================================================================

    /**
     * Validates that direction is OUTGOING or INCOMING for write operations.
     * Direction.BOTH is rejected with a clear error message.
     */
    static void validateWriteDirection(Direction direction) {
        if (direction == Direction.BOTH) {
            throw new IllegalArgumentException(
                    "Direction.BOTH is not supported for write operations (create, delete, migrate, flatten). "
                            + "Call the procedure twice with OUTGOING and INCOMING separately.");
        }
    }

    // ========================================================================
    // CREATE RELATIONSHIP
    // ========================================================================

    /**
     * Creates a relationship through the dense substructure.
     * If no substructure exists yet, initializes one.
     *
     * <p>Each leaf relationship stores {@code __dense_target} with the target node's
     * element ID for indexed delete lookup.</p>
     *
     * @param sourceNode  The dense node (source of the fan-out)
     * @param userType    The user-visible relationship type (e.g., "LIKES")
     * @param direction   Direction from sourceNode's perspective (OUTGOING or INCOMING only)
     * @param props       Properties to set on the created relationship
     * @param targetNode  The target node
     * @param config      Configuration for bucket capacity, branch factor, etc.
     * @return The created relationship and the bucket it was placed in
     * @throws IllegalArgumentException if direction is BOTH
     */
    public DenseResult.CreateRelationshipResult createRelationship(
            Node sourceNode,
            String userType,
            Direction direction,
            Map<String, Object> props,
            Node targetNode,
            DenseConfig config) {

        validateWriteDirection(direction);

        RelationshipType metaType = metaRelType(userType);
        RelationshipType leafType = leafRelType(userType);

        // Find or create the meta-relationship
        Relationship metaRel = findMetaRelationship(sourceNode, metaType, direction);

        if (metaRel == null) {
            // First time: initialize the entire substructure
            return initializeSubstructure(sourceNode, userType, direction, props, targetNode, config);
        }

        // --- OPTIMISTIC READ (no lock) ---
        String activeBucketId = (String) metaRel.getProperty(PROP_META_ACTIVE_BUCKET);
        Node activeBucket = tx.getNodeByElementId(activeBucketId);
        int currentCount = (int) activeBucket.getProperty(PROP_DENSE_COUNT);
        int capacity = (int) activeBucket.getProperty(PROP_DENSE_CAPACITY);

        if (currentCount < capacity) {
            // --- FAST PATH: lock bucket, re-check, write ---
            tx.acquireWriteLock(activeBucket);

            // Double-check after acquiring lock (another tx may have filled it)
            currentCount = (int) activeBucket.getProperty(PROP_DENSE_COUNT);
            if (currentCount < capacity) {
                Relationship rel = createLeafRelationship(activeBucket, targetNode, leafType, direction, props);
                activeBucket.setProperty(PROP_DENSE_COUNT, currentCount + 1);

                // Update total count on meta-rel (lock meta-rel for this)
                tx.acquireWriteLock(metaRel);
                long totalCount = (long) metaRel.getProperty(PROP_META_TOTAL_COUNT);
                metaRel.setProperty(PROP_META_TOTAL_COUNT, totalCount + 1);

                return new DenseResult.CreateRelationshipResult(rel, activeBucket);
            }
            // If bucket filled between optimistic read and lock, fall through to split path
        }

        // --- SPLIT PATH: strict lock ordering (meta-rel first, then buckets root->leaf) ---
        return splitAndCreate(sourceNode, metaRel, userType, direction, props, targetNode, config);
    }

    /**
     * Split path with strict lock ordering to prevent deadlocks.
     * Lock order: meta-relationship -> parent bucket -> new bucket
     */
    private DenseResult.CreateRelationshipResult splitAndCreate(
            Node sourceNode,
            Relationship metaRel,
            String userType,
            Direction direction,
            Map<String, Object> props,
            Node targetNode,
            DenseConfig config) {

        RelationshipType branchType = branchRelType(userType);
        RelationshipType leafType = leafRelType(userType);

        // Step 1: Lock meta-relationship FIRST (highest in hierarchy)
        tx.acquireWriteLock(metaRel);

        // Step 2: Re-read active bucket under meta lock
        String activeBucketId = (String) metaRel.getProperty(PROP_META_ACTIVE_BUCKET);
        Node activeBucket = tx.getNodeByElementId(activeBucketId);

        // Step 3: Lock active bucket (lower in hierarchy — safe order)
        tx.acquireWriteLock(activeBucket);

        int currentCount = (int) activeBucket.getProperty(PROP_DENSE_COUNT);
        int capacity = (int) activeBucket.getProperty(PROP_DENSE_CAPACITY);

        // Triple-check: maybe another tx already split
        if (currentCount < capacity) {
            Relationship rel = createLeafRelationship(activeBucket, targetNode, leafType, direction, props);
            activeBucket.setProperty(PROP_DENSE_COUNT, currentCount + 1);
            long totalCount = (long) metaRel.getProperty(PROP_META_TOTAL_COUNT);
            metaRel.setProperty(PROP_META_TOTAL_COUNT, totalCount + 1);
            return new DenseResult.CreateRelationshipResult(rel, activeBucket);
        }

        // Bucket is truly full — find the parent and determine if we need a new level
        int activeBucketLevel = (int) activeBucket.getProperty(PROP_DENSE_LEVEL);
        Node parentBucket = findParentBucket(activeBucket, branchType);

        Node newBucket;
        if (parentBucket != null) {
            // Lock parent (already holding meta-rel lock — parent is between meta and leaf)
            tx.acquireWriteLock(parentBucket);
            int parentChildCount = countChildren(parentBucket, branchType);

            if (parentChildCount < config.getBranchFactor()) {
                // Parent has room: create sibling bucket
                newBucket = createBucketNode(
                        sourceNode, userType, direction, activeBucketLevel, config.getBucketCapacity());
                parentBucket.createRelationshipTo(newBucket, branchType);
            } else {
                // Parent is full: create new parent at next level, add new leaf under it
                int currentLevels = (int) metaRel.getProperty(PROP_META_LEVELS);
                Node rootBucket = findRootBucket(metaRel);

                Node newBranchBucket = createBucketNode(
                        sourceNode, userType, direction, activeBucketLevel, config.getBranchFactor());
                newBucket = createBucketNode(
                        sourceNode, userType, direction, activeBucketLevel, config.getBucketCapacity());
                newBranchBucket.createRelationshipTo(newBucket, branchType);

                // Attach new branch to root (or create a new root if root is also full)
                if (rootBucket != null) {
                    tx.acquireWriteLock(rootBucket);
                    int rootChildCount = countChildren(rootBucket, branchType);
                    if (rootChildCount < config.getBranchFactor()) {
                        rootBucket.createRelationshipTo(newBranchBucket, branchType);
                    } else {
                        // Root is full — create new root, reparent
                        Node newRoot = createBucketNode(
                                sourceNode, userType, direction, currentLevels, config.getBranchFactor());
                        RelationshipType metaType = metaRelType(userType);
                        Relationship oldMeta = findMetaRelationship(sourceNode, metaType, direction);
                        Relationship newMeta = sourceNode.createRelationshipTo(newRoot, metaType);
                        copyMetaProperties(oldMeta, newMeta);
                        newRoot.createRelationshipTo(rootBucket, branchType);
                        newRoot.createRelationshipTo(newBranchBucket, branchType);
                        oldMeta.delete();
                        metaRel = newMeta;
                        metaRel.setProperty(PROP_META_LEVELS, currentLevels + 1);
                    }
                }
            }
        } else {
            // No parent means the active bucket IS the root — need to add a level
            int currentLevels = (int) metaRel.getProperty(PROP_META_LEVELS);
            Node newRoot = createBucketNode(sourceNode, userType, direction, 1, config.getBranchFactor());

            newBucket = createBucketNode(sourceNode, userType, direction, 0, config.getBucketCapacity());

            // Reparent: new root gets old root (activeBucket) and new leaf as children
            newRoot.createRelationshipTo(activeBucket, branchType);
            newRoot.createRelationshipTo(newBucket, branchType);

            // Rewire the meta-relationship to point to new root
            RelationshipType metaType = metaRelType(userType);
            Relationship newMeta = sourceNode.createRelationshipTo(newRoot, metaType);
            copyMetaProperties(metaRel, newMeta);
            metaRel.delete();
            metaRel = newMeta;
            metaRel.setProperty(PROP_META_LEVELS, currentLevels + 1);
        }

        // Deactivate old bucket, activate new one
        activeBucket.setProperty(PROP_DENSE_ACTIVE, false);
        newBucket.setProperty(PROP_DENSE_ACTIVE, true);
        metaRel.setProperty(PROP_META_ACTIVE_BUCKET, newBucket.getElementId());

        // Create the actual relationship on the new bucket
        Relationship rel = createLeafRelationship(newBucket, targetNode, leafType, direction, props);
        newBucket.setProperty(PROP_DENSE_COUNT, 1);
        long totalCount = (long) metaRel.getProperty(PROP_META_TOTAL_COUNT);
        metaRel.setProperty(PROP_META_TOTAL_COUNT, totalCount + 1);

        return new DenseResult.CreateRelationshipResult(rel, newBucket);
    }

    // ========================================================================
    // DELETE RELATIONSHIP (indexed lookup via __dense_target)
    // ========================================================================

    /**
     * Deletes a relationship from the dense substructure using indexed lookup.
     *
     * <p>Instead of scanning all leaf buckets linearly (O(total_relationships)),
     * this method uses the {@code __dense_target} property stored on each leaf
     * relationship. It finds all buckets for the source node via the
     * {@code __DenseBucket.__dense_source} index, then checks only relationships
     * matching the target's element ID.</p>
     *
     * <p>Complexity is O(buckets_for_source) in the worst case, which occurs when
     * the same source-target pair has multiple relationships of the same type spread
     * across different buckets. For typical workloads where each target appears in at
     * most one bucket, this is effectively O(bucket_capacity).</p>
     *
     * @param direction Must be OUTGOING or INCOMING (not BOTH)
     * @return result indicating if relationship was found and deleted
     * @throws IllegalArgumentException if direction is BOTH
     */
    public DenseResult.DeleteRelationshipResult deleteRelationship(
            Node sourceNode,
            String userType,
            Direction direction,
            Node targetNode,
            Map<String, Object> matchProps) {

        validateWriteDirection(direction);

        RelationshipType metaType = metaRelType(userType);
        Relationship metaRel = findMetaRelationship(sourceNode, metaType, direction);

        if (metaRel == null) {
            return new DenseResult.DeleteRelationshipResult(false, 0);
        }

        RelationshipType leafType = leafRelType(userType);
        String targetElementId = targetNode.getElementId();

        // Use source index to find buckets, then scan only matching-target rels
        boolean found = false;
        Node foundBucket = null;

        ResourceIterator<Node> buckets =
                tx.findNodes(BUCKET_LABEL, PROP_DENSE_SOURCE, sourceNode.getElementId());
        try {
            while (buckets.hasNext() && !found) {
                Node bucket = buckets.next();
                // Filter by type and direction
                String bucketType = (String) bucket.getProperty(PROP_DENSE_TYPE, "");
                String bucketDir = (String) bucket.getProperty(PROP_DENSE_DIRECTION, "");
                if (!userType.equals(bucketType) || !direction.name().equals(bucketDir)) {
                    continue;
                }
                // Check if this is a leaf bucket (has leaf rels, not just branch rels)
                Direction leafDir = (direction == Direction.OUTGOING) ? Direction.OUTGOING : Direction.INCOMING;
                for (Relationship rel : bucket.getRelationships(leafDir, leafType)) {
                    // Fast check: compare __dense_target property before checking otherNode
                    String storedTarget = (String) rel.getProperty(PROP_LEAF_TARGET, null);
                    if (targetElementId.equals(storedTarget) && propsMatch(rel, matchProps)) {
                        // Found it — lock bucket, delete, decrement
                        tx.acquireWriteLock(bucket);
                        rel.delete();
                        int count = (int) bucket.getProperty(PROP_DENSE_COUNT);
                        bucket.setProperty(PROP_DENSE_COUNT, count - 1);
                        found = true;
                        foundBucket = bucket;
                        break;
                    }
                }
            }
        } finally {
            buckets.close();
        }

        if (!found) {
            long totalCount = (long) metaRel.getProperty(PROP_META_TOTAL_COUNT);
            return new DenseResult.DeleteRelationshipResult(false, totalCount);
        }

        // Update total count
        tx.acquireWriteLock(metaRel);
        long totalCount = (long) metaRel.getProperty(PROP_META_TOTAL_COUNT);
        metaRel.setProperty(PROP_META_TOTAL_COUNT, totalCount - 1);

        // Compact: if bucket is now empty, remove it
        int remainingInBucket = (int) foundBucket.getProperty(PROP_DENSE_COUNT);
        if (remainingInBucket == 0) {
            RelationshipType branchType = branchRelType(userType);
            compactEmptyBucket(foundBucket, branchType, metaRel, sourceNode, userType, direction);
        }

        return new DenseResult.DeleteRelationshipResult(true, totalCount - 1);
    }

    /**
     * Removes an empty bucket and its branch relationship from the parent.
     * Recursively compacts parent if it becomes childless.
     */
    private void compactEmptyBucket(
            Node emptyBucket,
            RelationshipType branchType,
            Relationship metaRel,
            Node sourceNode,
            String userType,
            Direction direction) {
        // Find and remove the branch relationship pointing to this bucket
        for (Relationship branchRel : emptyBucket.getRelationships(Direction.INCOMING, branchType)) {
            Node parent = branchRel.getStartNode();
            branchRel.delete();

            // Check if parent has any remaining children
            if (!parent.getRelationships(Direction.OUTGOING, branchType)
                    .iterator()
                    .hasNext()) {
                // Parent is also empty — check if it's the root
                boolean isRoot = false;
                for (Relationship parentIncoming : parent.getRelationships(Direction.INCOMING)) {
                    if (parentIncoming.getType().name().startsWith(META_REL_PREFIX)) {
                        isRoot = true;
                        break;
                    }
                    if (parentIncoming.getType().name().startsWith(BRANCH_REL_PREFIX)) {
                        // Not root, recurse
                        compactEmptyBucket(parent, branchType, metaRel, sourceNode, userType, direction);
                        break;
                    }
                }
                if (!isRoot) {
                    for (Relationship r : parent.getRelationships()) {
                        r.delete();
                    }
                    parent.delete();
                }
            }
            break;
        }

        // Remove the empty bucket itself
        for (Relationship r : emptyBucket.getRelationships()) {
            r.delete();
        }
        emptyBucket.delete();
    }

    // ========================================================================
    // QUERY RELATIONSHIPS
    // ========================================================================

    /**
     * Streams relationships from the dense substructure with cursor-based pagination.
     * Supports Direction.BOTH by returning union of OUTGOING and INCOMING results.
     *
     * @param cursor  Opaque resume token (bucketElementId:position), or null for start
     * @param limit   Maximum number of results (0 = unlimited)
     * @return Stream of relationship query results with cursor tokens
     */
    public Stream<DenseResult.RelationshipQueryResult> queryRelationships(
            Node sourceNode, String userType, Direction direction, String cursor, long limit) {

        if (direction == Direction.BOTH) {
            // Union of OUTGOING and INCOMING
            Stream<DenseResult.RelationshipQueryResult> outgoing =
                    queryRelationshipsSingleDirection(sourceNode, userType, Direction.OUTGOING, cursor, limit);
            Stream<DenseResult.RelationshipQueryResult> incoming =
                    queryRelationshipsSingleDirection(sourceNode, userType, Direction.INCOMING, cursor, limit);
            return Stream.concat(outgoing, incoming);
        }

        return queryRelationshipsSingleDirection(sourceNode, userType, direction, cursor, limit);
    }

    private Stream<DenseResult.RelationshipQueryResult> queryRelationshipsSingleDirection(
            Node sourceNode, String userType, Direction direction, String cursor, long limit) {

        RelationshipType metaType = metaRelType(userType);
        Relationship metaRel = findMetaRelationship(sourceNode, metaType, direction);

        if (metaRel == null) {
            return Stream.empty();
        }

        RelationshipType leafType = leafRelType(userType);
        RelationshipType branchType = branchRelType(userType);

        Node rootBucket = findRootBucket(metaRel);
        if (rootBucket == null) {
            return Stream.empty();
        }

        List<Node> leafBuckets = collectLeafBuckets(rootBucket, branchType);

        // Parse cursor: "bucketElementId:position"
        int startBucketIndex = 0;
        int startPosition = 0;
        if (cursor != null && !cursor.isEmpty()) {
            String[] parts = cursor.split(":", 2);
            String cursorBucketId = parts[0];
            startPosition = parts.length > 1 ? Integer.parseInt(parts[1]) : 0;
            for (int i = 0; i < leafBuckets.size(); i++) {
                if (leafBuckets.get(i).getElementId().equals(cursorBucketId)) {
                    startBucketIndex = i;
                    break;
                }
            }
        }

        Direction leafDir = (direction == Direction.OUTGOING) ? Direction.OUTGOING : Direction.INCOMING;

        List<DenseResult.RelationshipQueryResult> results = new ArrayList<>();
        long count = 0;
        boolean limitReached = false;

        for (int bi = startBucketIndex; bi < leafBuckets.size() && !limitReached; bi++) {
            Node bucket = leafBuckets.get(bi);
            int pos = 0;
            for (Relationship rel : bucket.getRelationships(leafDir, leafType)) {
                if (bi == startBucketIndex && pos < startPosition) {
                    pos++;
                    continue;
                }
                pos++;
                count++;

                Node targetNode = rel.getOtherNode(bucket);
                String nextCursor = bucket.getElementId() + ":" + pos;
                results.add(new DenseResult.RelationshipQueryResult(rel, targetNode, nextCursor));

                if (limit > 0 && count >= limit) {
                    limitReached = true;
                    break;
                }
            }
        }

        return results.stream();
    }

    // ========================================================================
    // DEGREE (fast count via metadata)
    // ========================================================================

    /**
     * Returns the total relationship count from metadata without traversing the tree.
     * Supports Direction.BOTH by summing OUTGOING + INCOMING counts.
     */
    public long degree(Node sourceNode, String userType, Direction direction) {
        if (direction == Direction.BOTH) {
            return degree(sourceNode, userType, Direction.OUTGOING) + degree(sourceNode, userType, Direction.INCOMING);
        }
        RelationshipType metaType = metaRelType(userType);
        Relationship metaRel = findMetaRelationship(sourceNode, metaType, direction);
        if (metaRel == null) {
            return 0;
        }
        return (long) metaRel.getProperty(PROP_META_TOTAL_COUNT);
    }

    // ========================================================================
    // STATUS
    // ========================================================================

    /**
     * Returns status information about the dense substructure for a given node/type.
     * Supports Direction.BOTH by returning both OUTGOING and INCOMING status.
     */
    public Stream<DenseResult.StatusResult> statusStream(
            Node sourceNode, String userType, Direction direction) {
        if (direction == Direction.BOTH) {
            return Stream.concat(
                    statusStream(sourceNode, userType, Direction.OUTGOING),
                    statusStream(sourceNode, userType, Direction.INCOMING));
        }

        RelationshipType metaType = metaRelType(userType);
        Relationship metaRel = findMetaRelationship(sourceNode, metaType, direction);

        if (metaRel == null) {
            return Stream.empty();
        }

        RelationshipType branchType = branchRelType(userType);
        Node rootBucket = findRootBucket(metaRel);
        long bucketCount = rootBucket != null
                ? collectLeafBuckets(rootBucket, branchType).size()
                : 0;

        boolean migrating = false;
        if (metaRel.hasProperty(PROP_META_MIGRATION_IN_PROGRESS)) {
            migrating = (boolean) metaRel.getProperty(PROP_META_MIGRATION_IN_PROGRESS);
        }

        return Stream.of(new DenseResult.StatusResult(
                userType,
                direction.name(),
                (long) metaRel.getProperty(PROP_META_TOTAL_COUNT),
                (int) metaRel.getProperty(PROP_META_LEVELS),
                bucketCount,
                (int) metaRel.getProperty(PROP_META_BUCKET_CAPACITY),
                (int) metaRel.getProperty(PROP_META_BRANCH_FACTOR),
                migrating));
    }

    // ========================================================================
    // ANALYZE (detect dense nodes — streaming with sampling)
    // ========================================================================

    /**
     * Finds nodes exceeding the dense threshold. Streams results as they are
     * found — never buffers the full result set.
     *
     * <p>Supports probabilistic sampling via {@code sampleRate} (0.0–1.0) and
     * a hard result cap via {@code analyzeLimit} (default 500) to prevent OOM
     * on billion-node graphs.</p>
     */
    public Stream<DenseResult.AnalyzeResult> analyze(DenseConfig config) {
        int threshold = config.getDenseThreshold();
        String labelFilter = config.getLabel();
        double sampleRate = config.getSampleRate();
        int analyzeLimit = config.getAnalyzeLimit();

        ResourceIterator<Node> nodes;
        if (labelFilter != null && !labelFilter.isEmpty()) {
            nodes = tx.findNodes(Label.label(labelFilter));
        } else {
            nodes = tx.getAllNodes().iterator();
        }

        // Use Spliterator-based streaming to avoid buffering all results
        Iterator<DenseResult.AnalyzeResult> resultIterator = new Iterator<>() {
            private final Queue<DenseResult.AnalyzeResult> buffer = new LinkedList<>();
            private int yielded = 0;

            @Override
            public boolean hasNext() {
                if (yielded >= analyzeLimit) return false;
                while (buffer.isEmpty() && nodes.hasNext()) {
                    advance();
                }
                return !buffer.isEmpty();
            }

            @Override
            public DenseResult.AnalyzeResult next() {
                if (!hasNext()) throw new NoSuchElementException();
                yielded++;
                return buffer.poll();
            }

            private void advance() {
                Node node = nodes.next();

                // Probabilistic sampling
                if (sampleRate < 1.0 && ThreadLocalRandom.current().nextDouble() > sampleRate) {
                    return;
                }

                if (node.hasLabel(BUCKET_LABEL)) return; // Skip bucket nodes

                Map<String, Map<String, Long>> degreesPerTypeDir = new HashMap<>();

                for (Relationship rel : node.getRelationships()) {
                    String typeName = rel.getType().name();
                    if (typeName.startsWith("_DENSE_")) continue; // Skip internal rels

                    String dirKey = rel.getStartNode().equals(node) ? "OUTGOING" : "INCOMING";
                    degreesPerTypeDir
                            .computeIfAbsent(typeName, k -> new HashMap<>())
                            .merge(dirKey, 1L, Long::sum);
                }

                for (Map.Entry<String, Map<String, Long>> typeEntry : degreesPerTypeDir.entrySet()) {
                    for (Map.Entry<String, Long> dirEntry : typeEntry.getValue().entrySet()) {
                        if (dirEntry.getValue() >= threshold && yielded + buffer.size() < analyzeLimit) {
                            boolean alreadyManaged = findMetaRelationship(
                                            node,
                                            metaRelType(typeEntry.getKey()),
                                            Direction.valueOf(dirEntry.getKey()))
                                    != null;
                            buffer.add(new DenseResult.AnalyzeResult(
                                    node,
                                    typeEntry.getKey(),
                                    dirEntry.getKey(),
                                    dirEntry.getValue(),
                                    alreadyManaged));
                        }
                    }
                }
            }
        };

        Spliterator<DenseResult.AnalyzeResult> spliterator =
                Spliterators.spliteratorUnknownSize(resultIterator, Spliterator.ORDERED | Spliterator.NONNULL);
        return StreamSupport.stream(spliterator, false).onClose(nodes::close);
    }

    // ========================================================================
    // MIGRATE (move existing relationships into substructure)
    // ========================================================================

    /**
     * Migrates existing direct relationships into the dense substructure.
     * Processes in batches to avoid OOM.
     *
     * <p><strong>WARNING: Relationship element IDs are not preserved.</strong>
     * Migration deletes original relationships and creates new ones on bucket nodes.
     * Neo4j assigns new element IDs to the recreated relationships. Applications must
     * not cache relationship IDs for nodes that have been or may be migrated.
     * Use {@code apoc.dense.status()} to check migration state before caching.
     * This is consistent with how {@code apoc.refactor.*} procedures behave.</p>
     *
     * @param direction Must be OUTGOING or INCOMING (not BOTH)
     * @throws IllegalArgumentException if direction is BOTH
     */
    public DenseResult.MigrateResult migrate(
            Node sourceNode, String userType, Direction direction, DenseConfig config) {

        validateWriteDirection(direction);

        RelationshipType originalType = RelationshipType.withName(userType);
        RelationshipType metaType = metaRelType(userType);

        // Set migration flag
        Relationship metaRel = findMetaRelationship(sourceNode, metaType, direction);
        boolean isNew = (metaRel == null);

        if (isNew) {
            // Initialize empty substructure
            Node rootBucket =
                    createBucketNode(sourceNode, userType, direction, 0, config.getBucketCapacity());
            rootBucket.setProperty(PROP_DENSE_ACTIVE, true);
            metaRel = sourceNode.createRelationshipTo(rootBucket, metaType);
            initializeMetaProperties(metaRel, userType, direction, config);
            metaRel.setProperty(PROP_META_ACTIVE_BUCKET, rootBucket.getElementId());
        }

        metaRel.setProperty(PROP_META_MIGRATION_IN_PROGRESS, true);

        // Collect existing direct relationships to move
        Direction relDir = (direction == Direction.OUTGOING) ? Direction.OUTGOING : Direction.INCOMING;
        List<Relationship> toMigrate = new ArrayList<>();
        int batchCount = 0;
        for (Relationship rel : sourceNode.getRelationships(relDir, originalType)) {
            toMigrate.add(rel);
            batchCount++;
            if (batchCount >= config.getBatchSize()) break;
        }

        long migratedCount = 0;
        long bucketsCreated = isNew ? 1 : 0;

        for (Relationship existingRel : toMigrate) {
            Node targetNode = existingRel.getOtherNode(sourceNode);

            // Copy properties
            Map<String, Object> props = new HashMap<>();
            for (String key : existingRel.getPropertyKeys()) {
                props.put(key, existingRel.getProperty(key));
            }

            // Create through substructure
            createRelationship(sourceNode, userType, direction, props, targetNode, config);

            // Delete original
            existingRel.delete();
            migratedCount++;
        }

        // Check if migration is complete (no more direct rels of this type)
        boolean moreToMigrate =
                sourceNode.getRelationships(relDir, originalType).iterator().hasNext();

        if (!moreToMigrate) {
            metaRel = findMetaRelationship(sourceNode, metaType, direction);
            if (metaRel != null) {
                metaRel.setProperty(PROP_META_MIGRATION_IN_PROGRESS, false);
            }
        }

        int levels = 1;
        metaRel = findMetaRelationship(sourceNode, metaType, direction);
        if (metaRel != null) {
            levels = (int) metaRel.getProperty(PROP_META_LEVELS);
        }

        return new DenseResult.MigrateResult(migratedCount, bucketsCreated, levels, !moreToMigrate);
    }

    // ========================================================================
    // FLATTEN (reverse migration — dismantle substructure)
    // ========================================================================

    /**
     * Removes the dense substructure and reconnects relationships directly.
     * Processes in batches to avoid OOM.
     *
     * @param direction Must be OUTGOING or INCOMING (not BOTH)
     * @throws IllegalArgumentException if direction is BOTH
     */
    public DenseResult.FlattenResult flatten(
            Node sourceNode, String userType, Direction direction, DenseConfig config) {

        validateWriteDirection(direction);

        RelationshipType metaType = metaRelType(userType);
        RelationshipType leafType = leafRelType(userType);
        RelationshipType branchType = branchRelType(userType);
        RelationshipType originalType = RelationshipType.withName(userType);

        Relationship metaRel = findMetaRelationship(sourceNode, metaType, direction);
        if (metaRel == null) {
            return new DenseResult.FlattenResult(0, 0);
        }

        Node rootBucket = findRootBucket(metaRel);
        if (rootBucket == null) {
            return new DenseResult.FlattenResult(0, 0);
        }

        List<Node> leafBuckets = collectLeafBuckets(rootBucket, branchType);
        long flattenedCount = 0;
        long bucketsRemoved = 0;
        int batchCount = 0;

        Direction leafDir = (direction == Direction.OUTGOING) ? Direction.OUTGOING : Direction.INCOMING;

        for (Node bucket : leafBuckets) {
            List<Relationship> leafRels = new ArrayList<>();
            for (Relationship rel : bucket.getRelationships(leafDir, leafType)) {
                leafRels.add(rel);
            }

            for (Relationship leafRel : leafRels) {
                if (batchCount >= config.getBatchSize()) break;

                Node targetNode = leafRel.getOtherNode(bucket);
                Map<String, Object> props = new HashMap<>();
                for (String key : leafRel.getPropertyKeys()) {
                    // Skip internal properties when flattening back to direct rels
                    if (PROP_LEAF_TARGET.equals(key)) continue;
                    props.put(key, leafRel.getProperty(key));
                }

                // Recreate as direct relationship
                Relationship directRel;
                if (direction == Direction.OUTGOING) {
                    directRel = sourceNode.createRelationshipTo(targetNode, originalType);
                } else {
                    directRel = targetNode.createRelationshipTo(sourceNode, originalType);
                }
                for (Map.Entry<String, Object> entry : props.entrySet()) {
                    directRel.setProperty(entry.getKey(), entry.getValue());
                }

                leafRel.delete();
                flattenedCount++;
                batchCount++;

                int count = (int) bucket.getProperty(PROP_DENSE_COUNT);
                bucket.setProperty(PROP_DENSE_COUNT, count - 1);
            }

            // If bucket is now empty, remove it
            int remaining = (int) bucket.getProperty(PROP_DENSE_COUNT);
            if (remaining == 0) {
                compactEmptyBucket(bucket, branchType, metaRel, sourceNode, userType, direction);
                bucketsRemoved++;
            }
        }

        // If all relationships have been flattened, remove the meta-relationship
        metaRel = findMetaRelationship(sourceNode, metaType, direction);
        if (metaRel != null) {
            long totalCount = (long) metaRel.getProperty(PROP_META_TOTAL_COUNT);
            totalCount -= flattenedCount;
            if (totalCount <= 0) {
                // Clean up root bucket and meta-rel
                Node root = findRootBucket(metaRel);
                if (root != null) {
                    for (Relationship r : root.getRelationships()) {
                        r.delete();
                    }
                    root.delete();
                    bucketsRemoved++;
                }
                metaRel.delete();
            } else {
                metaRel.setProperty(PROP_META_TOTAL_COUNT, totalCount);
            }
        }

        return new DenseResult.FlattenResult(flattenedCount, bucketsRemoved);
    }

    // ========================================================================
    // PRIVATE HELPERS
    // ========================================================================

    private DenseResult.CreateRelationshipResult initializeSubstructure(
            Node sourceNode,
            String userType,
            Direction direction,
            Map<String, Object> props,
            Node targetNode,
            DenseConfig config) {

        RelationshipType metaType = metaRelType(userType);
        RelationshipType leafType = leafRelType(userType);

        // Create root bucket
        Node rootBucket = createBucketNode(sourceNode, userType, direction, 0, config.getBucketCapacity());
        rootBucket.setProperty(PROP_DENSE_ACTIVE, true);

        // Create meta-relationship from source -> root bucket
        Relationship metaRel = sourceNode.createRelationshipTo(rootBucket, metaType);
        initializeMetaProperties(metaRel, userType, direction, config);
        metaRel.setProperty(PROP_META_ACTIVE_BUCKET, rootBucket.getElementId());
        metaRel.setProperty(PROP_META_TOTAL_COUNT, 1L);

        // Create the actual leaf relationship
        Relationship rel = createLeafRelationship(rootBucket, targetNode, leafType, direction, props);
        rootBucket.setProperty(PROP_DENSE_COUNT, 1);

        return new DenseResult.CreateRelationshipResult(rel, rootBucket);
    }

    private void initializeMetaProperties(
            Relationship metaRel, String userType, Direction direction, DenseConfig config) {
        metaRel.setProperty(PROP_META_TYPE, userType);
        metaRel.setProperty(PROP_META_DIRECTION, direction.name());
        metaRel.setProperty(PROP_META_TOTAL_COUNT, 0L);
        metaRel.setProperty(PROP_META_LEVELS, 1);
        metaRel.setProperty(PROP_META_BRANCH_FACTOR, config.getBranchFactor());
        metaRel.setProperty(PROP_META_BUCKET_CAPACITY, config.getBucketCapacity());
        metaRel.setProperty(PROP_META_MIGRATION_IN_PROGRESS, false);
    }

    private Node createBucketNode(
            Node sourceNode, String userType, Direction direction, int level, int capacity) {
        Node bucket = tx.createNode(BUCKET_LABEL);
        bucket.setProperty(PROP_DENSE_SOURCE, sourceNode.getElementId());
        bucket.setProperty(PROP_DENSE_TYPE, userType);
        bucket.setProperty(PROP_DENSE_DIRECTION, direction.name());
        bucket.setProperty(PROP_DENSE_LEVEL, level);
        bucket.setProperty(PROP_DENSE_COUNT, 0);
        bucket.setProperty(PROP_DENSE_CAPACITY, capacity);
        bucket.setProperty(PROP_DENSE_ACTIVE, false);
        return bucket;
    }

    /**
     * Creates a leaf relationship and stores __dense_target for indexed delete lookup.
     */
    private Relationship createLeafRelationship(
            Node bucket, Node target, RelationshipType leafType, Direction direction, Map<String, Object> props) {
        Relationship rel;
        if (direction == Direction.OUTGOING) {
            rel = bucket.createRelationshipTo(target, leafType);
        } else {
            rel = target.createRelationshipTo(bucket, leafType);
        }
        // Store target element ID for indexed delete lookup
        rel.setProperty(PROP_LEAF_TARGET, target.getElementId());
        if (props != null) {
            for (Map.Entry<String, Object> entry : props.entrySet()) {
                rel.setProperty(entry.getKey(), entry.getValue());
            }
        }
        return rel;
    }

    /**
     * Finds the meta-relationship for a given source node, type, and direction.
     */
    Relationship findMetaRelationship(Node sourceNode, RelationshipType metaType, Direction direction) {
        for (Relationship rel : sourceNode.getRelationships(Direction.OUTGOING, metaType)) {
            String storedDir = (String) rel.getProperty(PROP_META_DIRECTION, null);
            if (storedDir != null && storedDir.equals(direction.name())) {
                return rel;
            }
        }
        return null;
    }

    private Node findRootBucket(Relationship metaRel) {
        return metaRel.getEndNode();
    }

    private Node findParentBucket(Node bucket, RelationshipType branchType) {
        for (Relationship rel : bucket.getRelationships(Direction.INCOMING, branchType)) {
            Node parent = rel.getStartNode();
            if (parent.hasLabel(BUCKET_LABEL)) {
                return parent;
            }
        }
        return null;
    }

    private int countChildren(Node bucket, RelationshipType branchType) {
        int count = 0;
        for (Relationship ignored : bucket.getRelationships(Direction.OUTGOING, branchType)) {
            count++;
        }
        return count;
    }

    List<Node> collectLeafBuckets(Node rootBucket, RelationshipType branchType) {
        List<Node> leaves = new ArrayList<>();
        Queue<Node> queue = new LinkedList<>();
        queue.add(rootBucket);

        while (!queue.isEmpty()) {
            Node current = queue.poll();
            boolean hasChildren = false;

            for (Relationship branchRel : current.getRelationships(Direction.OUTGOING, branchType)) {
                queue.add(branchRel.getEndNode());
                hasChildren = true;
            }

            if (!hasChildren) {
                leaves.add(current);
            }
        }

        return leaves;
    }

    private boolean propsMatch(Relationship rel, Map<String, Object> matchProps) {
        if (matchProps == null || matchProps.isEmpty()) {
            return true;
        }
        for (Map.Entry<String, Object> entry : matchProps.entrySet()) {
            Object relValue = rel.getProperty(entry.getKey(), null);
            if (!Objects.equals(relValue, entry.getValue())) {
                return false;
            }
        }
        return true;
    }

    private void copyMetaProperties(Relationship source, Relationship target) {
        for (String key : source.getPropertyKeys()) {
            target.setProperty(key, source.getProperty(key));
        }
    }
}
