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
package apoc.util.kernel;

import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PartitionedScan;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.ExecutionContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;

public class MultiThreadedGlobalGraphOperationsExtended {

    public static BatchJobResult forAllNodes(
            GraphDatabaseAPI db, ExecutorService executorService, int batchSize, Consumer<NodeCursor> consumer) {
        BatchJobResult result = new BatchJobResult();
        AtomicInteger processing = new AtomicInteger();
        try (InternalTransaction tx =
                db.beginTransaction(KernelTransaction.Type.EXPLICIT, LoginContext.AUTH_DISABLED)) {
            KernelTransaction ktx = tx.kernelTransaction();
            Read dataRead = ktx.dataRead();
            PartitionedScan<NodeCursor> scan = dataRead.allNodesScan(1, ktx.cursorContext());
            Function<KernelTransaction, NodeCursor> cursorAllocator =
                    ktx2 -> ktx2.cursors().allocateNodeCursor(ktx2.cursorContext());
            executorService.submit(
                    new BatchJob(scan, batchSize, db, consumer, result, cursorAllocator, executorService, processing));
        }

        try {
            while (processing.get() > 0) {
                Thread.sleep(10);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return result;
    }

    public static class BatchJobResult {
        final AtomicInteger batches = new AtomicInteger();
        final AtomicLong succeeded = new AtomicLong();
        final AtomicLong failures = new AtomicLong();

        public void incrementSuceeded() {
            succeeded.incrementAndGet();
        }

        public void incrementFailures() {
            failures.incrementAndGet();
        }

        public long getSucceeded() {
            return succeeded.get();
        }

        public long getFailures() {
            return failures.get();
        }
    }

    private static class BatchJob implements Callable<Void> {
        private final PartitionedScan<NodeCursor> scan;
        private final int batchSize;
        private final GraphDatabaseAPI db;
        private final Consumer<NodeCursor> consumer;
        private final BatchJobResult result;
        private final Function<KernelTransaction, NodeCursor> cursorAllocator;
        private final ExecutorService executorService;
        private final AtomicInteger processing;

        public BatchJob(
                PartitionedScan<NodeCursor> scan,
                int batchSize,
                GraphDatabaseAPI db,
                Consumer<NodeCursor> consumer,
                BatchJobResult result,
                Function<KernelTransaction, NodeCursor> cursorAllocator,
                ExecutorService executorService,
                AtomicInteger processing) {
            this.scan = scan;
            this.batchSize = batchSize;
            this.db = db;
            this.consumer = consumer;
            this.result = result;
            this.cursorAllocator = cursorAllocator;
            this.executorService = executorService;
            this.processing = processing;
            processing.incrementAndGet();
        }

        @Override
        public Void call() {
            try (InternalTransaction tx =
                    db.beginTransaction(KernelTransaction.Type.EXPLICIT, LoginContext.AUTH_DISABLED)) {
                KernelTransaction ktx = tx.kernelTransaction();
                ktx.acquireStatement();
                ExecutionContext executionContext = ktx.createExecutionContext();
                try (NodeCursor cursor = cursorAllocator.apply(ktx)) {
                    while (scan.reservePartition(cursor, executionContext)) {
                        // Branch out so that all available threads will get saturated
                        executorService.submit(new BatchJob(
                                scan, batchSize, db, consumer, result, cursorAllocator, executorService, processing));
                        executorService.submit(new BatchJob(
                                scan, batchSize, db, consumer, result, cursorAllocator, executorService, processing));
                        while (processAndReport(cursor)) {
                            // just continue processing...
                        }
                    }
                }
                tx.commit();
                executionContext.complete();
                executionContext.close();
                return null;
            } finally {
                result.batches.incrementAndGet();
                processing.decrementAndGet();
            }
        }

        private boolean processAndReport(NodeCursor cursor) {
            if (cursor.next()) {
                try {
                    consumer.accept(cursor);
                    result.incrementSuceeded();
                } catch (Exception e) {
                    result.incrementFailures();
                }
                return true;
            }
            return false;
        }
    }
}
