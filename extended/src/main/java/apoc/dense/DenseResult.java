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

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

/**
 * Result types for dense node substructure procedures.
 * Follows APOC convention of public final fields with simple constructors.
 */
public class DenseResult {

    /**
     * Result for apoc.dense.create.relationship
     */
    public static class CreateRelationshipResult {
        public final Relationship rel;
        public final Node bucket;

        public CreateRelationshipResult(Relationship rel, Node bucket) {
            this.rel = rel;
            this.bucket = bucket;
        }
    }

    /**
     * Result for apoc.dense.delete.relationship
     */
    public static class DeleteRelationshipResult {
        public final boolean removed;
        public final long remainingCount;

        public DeleteRelationshipResult(boolean removed, long remainingCount) {
            this.removed = removed;
            this.remainingCount = remainingCount;
        }
    }

    /**
     * Result for apoc.dense.relationships (query)
     */
    public static class RelationshipQueryResult {
        public final Relationship rel;
        public final Node node;
        public final String cursor;

        public RelationshipQueryResult(Relationship rel, Node node, String cursor) {
            this.rel = rel;
            this.node = node;
            this.cursor = cursor;
        }
    }

    /**
     * Result for apoc.dense.analyze
     */
    public static class AnalyzeResult {
        public final Node node;
        public final String type;
        public final String direction;
        public final long degree;
        public final boolean alreadyManaged;

        public AnalyzeResult(Node node, String type, String direction, long degree, boolean alreadyManaged) {
            this.node = node;
            this.type = type;
            this.direction = direction;
            this.degree = degree;
            this.alreadyManaged = alreadyManaged;
        }
    }

    /**
     * Result for apoc.dense.migrate
     */
    public static class MigrateResult {
        public final long migratedCount;
        public final long bucketsCreated;
        public final int levels;
        public final boolean migrationComplete;

        public MigrateResult(long migratedCount, long bucketsCreated, int levels, boolean migrationComplete) {
            this.migratedCount = migratedCount;
            this.bucketsCreated = bucketsCreated;
            this.levels = levels;
            this.migrationComplete = migrationComplete;
        }
    }

    /**
     * Result for apoc.dense.flatten
     */
    public static class FlattenResult {
        public final long flattenedCount;
        public final long bucketsRemoved;

        public FlattenResult(long flattenedCount, long bucketsRemoved) {
            this.flattenedCount = flattenedCount;
            this.bucketsRemoved = bucketsRemoved;
        }
    }

    /**
     * Result for apoc.dense.status
     */
    public static class StatusResult {
        public final String type;
        public final String direction;
        public final long totalCount;
        public final int levels;
        public final long bucketCount;
        public final int bucketCapacity;
        public final int branchFactor;
        public final boolean migrationInProgress;

        public StatusResult(
                String type,
                String direction,
                long totalCount,
                int levels,
                long bucketCount,
                int bucketCapacity,
                int branchFactor,
                boolean migrationInProgress) {
            this.type = type;
            this.direction = direction;
            this.totalCount = totalCount;
            this.levels = levels;
            this.bucketCount = bucketCount;
            this.bucketCapacity = bucketCapacity;
            this.branchFactor = branchFactor;
            this.migrationInProgress = migrationInProgress;
        }
    }
}
