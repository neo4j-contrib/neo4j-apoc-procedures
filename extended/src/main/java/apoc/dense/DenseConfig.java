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

import java.util.Collections;
import java.util.Map;

/**
 * Configuration for dense node substructure operations.
 * Follows the APOC config-map convention (e.g., RefactorConfig, NodesConfig).
 */
public class DenseConfig {

    private final int bucketCapacity;
    private final int branchFactor;
    private final int denseThreshold;
    private final int batchSize;
    private final boolean autoMigrate;
    private final String label;
    private final long limit;
    private final String cursor;
    private final double sampleRate;
    private final int analyzeLimit;

    public DenseConfig(Map<String, Object> config) {
        if (config == null) {
            config = Collections.emptyMap();
        }
        this.bucketCapacity = toInt(config.getOrDefault("bucketCapacity", DEFAULT_BUCKET_CAPACITY));
        this.branchFactor = toInt(config.getOrDefault("branchFactor", DEFAULT_BRANCH_FACTOR));
        this.denseThreshold = toInt(config.getOrDefault("denseThreshold", DEFAULT_DENSE_THRESHOLD));
        this.batchSize = toInt(config.getOrDefault("batchSize", DEFAULT_BATCH_SIZE));
        this.autoMigrate = toBoolean(config.getOrDefault("autoMigrate", false));
        this.label = (String) config.getOrDefault("label", null);
        this.limit = toLong(config.getOrDefault("limit", 0L));
        this.cursor = (String) config.getOrDefault("cursor", null);
        this.sampleRate = toDouble(config.getOrDefault("sampleRate", DEFAULT_SAMPLE_RATE));
        this.analyzeLimit = toInt(config.getOrDefault("analyzeLimit", DEFAULT_ANALYZE_LIMIT));
    }

    public int getBucketCapacity() {
        return bucketCapacity;
    }

    public int getBranchFactor() {
        return branchFactor;
    }

    public int getDenseThreshold() {
        return denseThreshold;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public boolean isAutoMigrate() {
        return autoMigrate;
    }

    public String getLabel() {
        return label;
    }

    public long getLimit() {
        return limit;
    }

    public String getCursor() {
        return cursor;
    }

    public double getSampleRate() {
        return sampleRate;
    }

    public int getAnalyzeLimit() {
        return analyzeLimit;
    }

    private static int toInt(Object value) {
        if (value instanceof Number) {
            return ((Number) value).intValue();
        }
        if (value instanceof String) {
            return Integer.parseInt((String) value);
        }
        return 0;
    }

    private static long toLong(Object value) {
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        if (value instanceof String) {
            return Long.parseLong((String) value);
        }
        return 0L;
    }

    private static double toDouble(Object value) {
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }
        if (value instanceof String) {
            return Double.parseDouble((String) value);
        }
        return 0.0;
    }

    private static boolean toBoolean(Object value) {
        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        if (value instanceof String) {
            return Boolean.parseBoolean((String) value);
        }
        return false;
    }
}
