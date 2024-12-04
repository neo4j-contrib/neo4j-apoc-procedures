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

import apoc.util.UtilExtended;
import org.neo4j.procedure.Description;

import java.util.List;
import java.util.Map;

public class BatchAndTotalResultExtended {
    @Description("The total number of batches.")
    public final long batches;

    @Description("The number of processed input rows.")
    public final long total;

    @Description("The duration taken in seconds.")
    public final long timeTaken;

    @Description("The number of successful inner queries (actions).")
    public final long committedOperations;

    @Description("The number of failed inner queries (actions).")
    public final long failedOperations;

    @Description("The number of failed batches.")
    public final long failedBatches;

    @Description("The number of retries.")
    public final long retries;

    @Description("A map of batch error messages paired with their corresponding error counts.")
    public final Map<String, Long> errorMessages;

    @Description(
            """
            {
                 total :: INTEGER,
                 failed :: INTEGER,
                 committed :: INTEGER,
                 errors :: MAP
            }
            """)
    public final Map<String, Object> batch;

    @Description(
            """
            {
                 total :: INTEGER,
                 failed :: INTEGER,
                 committed :: INTEGER,
                 errors :: MAP
            }
            """)
    public final Map<String, Object> operations;

    @Description("If the transaction was terminated before completion.")
    public final boolean wasTerminated;

    @Description(
            "Parameters of failed batches. The key is the batch number as a STRING and the value is a list of batch parameters.")
    public final Map<String, List<Map<String, Object>>> failedParams;

    @Description(
            """
            {
                nodesCreated :: INTEGER,
                nodesDeleted :: INTEGER,
                relationshipsCreated :: INTEGER,
                relationshipsDeleted :: INTEGER,
                propertiesSet :: INTEGER,
                labelsAdded :: INTEGER,
                labelsRemoved :: INTEGER
            }
            """)
    public final Map<String, Long> updateStatistics;

    public BatchAndTotalResultExtended(
            long batches,
            long total,
            long timeTaken,
            long committedOperations,
            long failedOperations,
            long failedBatches,
            long retries,
            Map<String, Long> operationErrors,
            Map<String, Long> batchErrors,
            boolean wasTerminated,
            Map<String, List<Map<String, Object>>> failedParams,
            Map<String, Long> updateStatistics) {
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
        this.batch = UtilExtended.map(
                "total", batches, "failed", failedBatches, "committed", batches - failedBatches, "errors", batchErrors);
        this.operations = UtilExtended.map(
                "total",
                total,
                "failed",
                failedOperations,
                "committed",
                committedOperations,
                "errors",
                operationErrors);
        this.updateStatistics = updateStatistics;
    }
}
