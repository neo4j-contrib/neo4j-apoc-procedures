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
package apoc.periodic;

import apoc.util.Util;

import java.util.List;
import java.util.Map;

public class BatchAndTotalResult {
    public final long batches;
    public final long total;
    public final long timeTaken;
    public final long committedOperations;
    public final long failedOperations;
    public final long failedBatches;
    public final long retries;
    public final Map<String,Long> errorMessages;
    public final Map<String,Object> batch;
    public final Map<String,Object> operations;
    public final boolean wasTerminated;
    public final Map<String, List<Map<String,Object>>> failedParams;
    public final Map<String, Long> updateStatistics;

    public BatchAndTotalResult(long batches, long total, long timeTaken, long committedOperations,
                               long failedOperations, long failedBatches, long retries,
                               Map<String, Long> operationErrors, Map<String, Long> batchErrors, boolean wasTerminated,
                               Map<String, List<Map<String, Object>>> failedParams, Map<String, Long> updateStatistics) {
        this.batches = batches;
        this.total = total;
        this.timeTaken = timeTaken;
        this.committedOperations = committedOperations;
        this.failedOperations = failedOperations;
        this.failedBatches = failedBatches;
        this.retries = retries;
        this.errorMessages = operationErrors;
        this.wasTerminated = wasTerminated;
        this.failedParams = failedParams;
        this.batch = Util.map("total",batches,"failed",failedBatches,"committed",batches-failedBatches,"errors",batchErrors);
        this.operations = Util.map("total",total,"failed",failedOperations,"committed", committedOperations,"errors",operationErrors);
        this.updateStatistics = updateStatistics;
    }

    public LoopingBatchAndTotalResult inLoop(Object loop) {
        return new LoopingBatchAndTotalResult(loop, batches, total);
    }
}
