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

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;

/**
 * Constants for the dense node substructure system.
 * Uses double-underscore prefix convention matching Neo4j's internal labels (e.g., __Neo4jMigration).
 */
public final class DenseConstants {

    private DenseConstants() {}

    // -- Labels --
    public static final String BUCKET_LABEL_NAME = "__DenseBucket";
    public static final Label BUCKET_LABEL = Label.label(BUCKET_LABEL_NAME);

    // -- Internal relationship type prefixes --
    public static final String META_REL_PREFIX = "_DENSE_META_";
    public static final String BRANCH_REL_PREFIX = "_DENSE_BRANCH_";
    public static final String LEAF_REL_PREFIX = "_DENSE_";

    // -- Bucket node properties --
    public static final String PROP_DENSE_SOURCE = "__dense_source";
    public static final String PROP_DENSE_TYPE = "__dense_type";
    public static final String PROP_DENSE_DIRECTION = "__dense_direction";
    public static final String PROP_DENSE_LEVEL = "__dense_level";
    public static final String PROP_DENSE_COUNT = "__dense_count";
    public static final String PROP_DENSE_CAPACITY = "__dense_capacity";
    public static final String PROP_DENSE_ACTIVE = "__dense_active";

    // -- Meta-relationship properties --
    public static final String PROP_META_TYPE = "__dense_type";
    public static final String PROP_META_DIRECTION = "__dense_direction";
    public static final String PROP_META_TOTAL_COUNT = "__dense_total_count";
    public static final String PROP_META_LEVELS = "__dense_levels";
    public static final String PROP_META_BRANCH_FACTOR = "__dense_branch_factor";
    public static final String PROP_META_BUCKET_CAPACITY = "__dense_bucket_capacity";
    public static final String PROP_META_ACTIVE_BUCKET = "__dense_active_bucket";
    public static final String PROP_META_MIGRATION_IN_PROGRESS = "__dense_migration_in_progress";

    // -- Leaf relationship properties (for indexed delete lookup) --
    public static final String PROP_LEAF_TARGET = "__dense_target";

    // -- Index names --
    public static final String SOURCE_INDEX_NAME = "__dense_bucket_source_idx";

    // -- Defaults --
    public static final int DEFAULT_BUCKET_CAPACITY = 1000;
    public static final int DEFAULT_BRANCH_FACTOR = 100;
    public static final int DEFAULT_DENSE_THRESHOLD = 10000;
    public static final int DEFAULT_BATCH_SIZE = 5000;
    public static final int DEFAULT_ANALYZE_LIMIT = 500;
    public static final double DEFAULT_SAMPLE_RATE = 1.0;

    /**
     * Returns the meta-relationship type for a given user relationship type.
     * E.g., "LIKES" -> "_DENSE_META_LIKES"
     */
    public static RelationshipType metaRelType(String userType) {
        return RelationshipType.withName(META_REL_PREFIX + userType);
    }

    /**
     * Returns the branch relationship type for a given user relationship type.
     * E.g., "LIKES" -> "_DENSE_BRANCH_LIKES"
     */
    public static RelationshipType branchRelType(String userType) {
        return RelationshipType.withName(BRANCH_REL_PREFIX + userType);
    }

    /**
     * Returns the leaf (dense) relationship type for a given user relationship type.
     * E.g., "LIKES" -> "_DENSE_LIKES"
     */
    public static RelationshipType leafRelType(String userType) {
        return RelationshipType.withName(LEAF_REL_PREFIX + userType);
    }

    /**
     * Extracts the original user relationship type from a prefixed dense type.
     * E.g., "_DENSE_LIKES" -> "LIKES"
     */
    public static String extractUserType(String denseType) {
        if (denseType.startsWith(LEAF_REL_PREFIX)) {
            return denseType.substring(LEAF_REL_PREFIX.length());
        }
        if (denseType.startsWith(META_REL_PREFIX)) {
            return denseType.substring(META_REL_PREFIX.length());
        }
        if (denseType.startsWith(BRANCH_REL_PREFIX)) {
            return denseType.substring(BRANCH_REL_PREFIX.length());
        }
        return denseType;
    }
}
