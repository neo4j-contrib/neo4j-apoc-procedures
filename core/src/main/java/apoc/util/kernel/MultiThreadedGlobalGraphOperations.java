package apoc.util.kernel;

import org.neo4j.common.DependencyResolver;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.recordstorage.RecordStorageEngine;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.store.CommonAbstractStore;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

public class MultiThreadedGlobalGraphOperations {

    public enum GlobalOperationsTypes { NODES, RELATIONSHIPS }

    public static BatchJobResult forAllNodes(GraphDatabaseAPI db, ExecutorService executorService, int batchSize, BiConsumer<KernelTransaction, NodeCursor> consumer) {
        return forAll(db, executorService, batchSize, GlobalOperationsTypes.NODES, consumer);
    }

    public static BatchJobResult forAllRelationships(GraphDatabaseAPI db, ExecutorService executorService, int batchSize, BiConsumer<KernelTransaction, RelationshipScanCursor> consumer) {
        return forAll(db, executorService, batchSize, GlobalOperationsTypes.RELATIONSHIPS, consumer);
    }

    private static BatchJobResult forAll(GraphDatabaseAPI db, ExecutorService executorService, int batchSize, GlobalOperationsTypes type, BiConsumer consumer) {
        try {
            DependencyResolver dependencyResolver = db.getDependencyResolver();
            long maxId = getHighestIdInUseForStore(dependencyResolver, type);

            List<BatchJob> taskList = new ArrayList<>();
            BatchJobResult result = new BatchJobResult();

            result.startStopWatch();
            for (long batchStart = 0; batchStart < maxId; batchStart += batchSize) {
                taskList.add(new BatchJob(type, batchStart, batchSize, db, consumer, result));
            }
            executorService.invokeAll(taskList);
            result.stopStopWatch();
            result.setBatches(taskList.size());
            return result;

        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static long getHighestIdInUseForStore(DependencyResolver dependencyResolver, GlobalOperationsTypes type) {
        NeoStores neoStores = dependencyResolver.resolveDependency(RecordStorageEngine.class).testAccessNeoStores();
        CommonAbstractStore store;
        switch (type) {
            case NODES:
                store = neoStores.getNodeStore();
                break;
            case RELATIONSHIPS:
                store = neoStores.getRelationshipStore();
                break;
            default:
                throw new IllegalArgumentException("invalid type " + type);
        }
        return store.getHighId();
    }

    public static class BatchJobResult {
        final AtomicLong succeeded = new AtomicLong(0);
        final AtomicLong missing = new AtomicLong( 0);
        final AtomicLong failures = new AtomicLong(0);
        private long started;
        private long duration;
        private int batches;

        public void incrementSuceeded() {
            succeeded.incrementAndGet();
        }

        public void incrementMissing() {
            missing.incrementAndGet();
        }

        public void incrementFailures() {
            failures.incrementAndGet();
        }

        public long getSucceeded() {
            return succeeded.get();
        }

        public long getMissing() {
            return missing.get();
        }

        public long getFailures() {
            return failures.get();
        }

        public long getDuration() {
            return duration;
        }

        public void startStopWatch() {
            started = System.currentTimeMillis();
        }

        public void stopStopWatch() {
            duration = System.currentTimeMillis() - started;
        }

        public void setBatches(int batches) {
            this.batches = batches;
        }

        public int getBatches() {
            return batches;
        }
    }

    private static class BatchJob implements Callable<Void> {
        private final GlobalOperationsTypes type;
        private final long batchStart;
        private final int batchSize;
        private final GraphDatabaseAPI db;
        private final BiConsumer consumer;
        private final BatchJobResult result;

        public BatchJob(GlobalOperationsTypes type, long batchStart, int batchSize, GraphDatabaseAPI db, BiConsumer consumer, BatchJobResult result) {
            this.type = type;
            this.batchStart = batchStart;
            this.batchSize = batchSize;
            this.db = db;
            this.consumer = consumer;
            this.result = result;
        }

        @Override
        public Void call() {
            try (Transaction tx = db.beginTx()) {
                KernelTransaction ktx = ((InternalTransaction)tx).kernelTransaction();
                CursorFactory cursors = ktx.cursors();
                Read read = ktx.dataRead();

                switch (type) {
                    case NODES:
                        iterateForNodes(ktx, read, cursors, result);
                        break;
                    case RELATIONSHIPS:
                        iterateForRelationships(ktx, read, cursors, result);
                        break;
                    default:
                        throw new IllegalArgumentException("dunno how to deal with type " + type);

                }
                tx.commit();
                return null;
            }
        }

        private void iterateForNodes(KernelTransaction ktx, Read read, CursorFactory cursors, BatchJobResult result) {
            try (NodeCursor cursor = cursors.allocateNodeCursor(ktx.cursorContext())) {
                for (long id = batchStart; id < batchStart + batchSize; id++) {
                    read.singleNode(id, cursor);
                    processAndReport(ktx, cursor::next, consumer, cursor, result);
                }
            }
        }

        private void iterateForRelationships(KernelTransaction ktx, Read read, CursorFactory cursors, BatchJobResult result) {
            try (RelationshipScanCursor cursor = cursors.allocateRelationshipScanCursor(ktx.cursorContext())) {
                for (long id = batchStart; id < batchStart + batchSize; id++) {
                    read.singleRelationship(id, cursor);
                    processAndReport(ktx, cursor::next, consumer, cursor, result);
                }
            }
        }

        private void processAndReport(KernelTransaction ktx, Supplier<Boolean> nextMethod, BiConsumer consumer, Object parameter, BatchJobResult result) {
            if (nextMethod.get()) {
                try {
                    consumer.accept(ktx, parameter);
                    result.incrementSuceeded();
                } catch (Exception e) {
                    result.incrementFailures();
                }
            } else {
                result.incrementMissing();
            }
        }

    }
}
