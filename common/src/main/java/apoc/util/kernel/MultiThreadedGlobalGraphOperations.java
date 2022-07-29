package apoc.util.kernel;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.neo4j.internal.kernel.api.EntityCursor;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.Scan;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

public class MultiThreadedGlobalGraphOperations {
    public static BatchJobResult forAllNodes(GraphDatabaseAPI db, ExecutorService executorService, int batchSize, BiConsumer<KernelTransaction,
            NodeCursor> consumer) {
        return forAll(db, executorService, batchSize, consumer, Read::allNodesScan, ktx -> ktx.cursors().allocateNodeCursor( ktx.cursorContext() ));
    }

    public static BatchJobResult forAllRelationships(GraphDatabaseAPI db, ExecutorService executorService, int batchSize, BiConsumer<KernelTransaction,
            RelationshipScanCursor> consumer) {
        return forAll( db, executorService, batchSize, consumer, Read::allRelationshipsScan,
                ktx -> ktx.cursors().allocateRelationshipScanCursor( ktx.cursorContext() ) );
    }

    private static <C extends EntityCursor> BatchJobResult forAll( GraphDatabaseAPI db, ExecutorService executorService, int batchSize,
            BiConsumer<KernelTransaction,C> consumer, Function<Read, Scan<C>> scanFunction, Function<KernelTransaction,C> cursorAllocator ) {
        BatchJobResult result = new BatchJobResult();
        AtomicInteger processing = new AtomicInteger();
        try ( InternalTransaction tx = db.beginTransaction( KernelTransaction.Type.EXPLICIT, LoginContext.AUTH_DISABLED ) ) {
            KernelTransaction ktx = tx.kernelTransaction();
            Scan<C> scan = scanFunction.apply( ktx.dataRead() );
            result.startStopWatch();
            executorService.submit( new BatchJob<>( scan, batchSize, db, consumer, result, cursorAllocator, executorService, processing ) );
        }

        try {
            while ( processing.get() > 0 ) {
                Thread.sleep( 10 );
            }
            result.stopStopWatch();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return result;
    }

    public static class BatchJobResult {
        final AtomicInteger batches = new AtomicInteger();
        final AtomicLong succeeded = new AtomicLong();
        final AtomicLong failures = new AtomicLong();
        private long started;
        private long duration;

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

        public long getDuration() {
            return duration;
        }

        public void startStopWatch() {
            started = System.currentTimeMillis();
        }

        public void stopStopWatch() {
            duration = System.currentTimeMillis() - started;
        }

        public int getBatches() {
            return batches.get();
        }
    }

    private static class BatchJob<C extends EntityCursor> implements Callable<Void> {
        private final Scan<C> scan;
        private final int batchSize;
        private final GraphDatabaseAPI db;
        private final BiConsumer<KernelTransaction,C> consumer;
        private final BatchJobResult result;
        private final Function<KernelTransaction,C> cursorAllocator;
        private final ExecutorService executorService;
        private final AtomicInteger processing;

        public BatchJob(Scan<C> scan, int batchSize, GraphDatabaseAPI db, BiConsumer<KernelTransaction,C> consumer,
                BatchJobResult result, Function<KernelTransaction,C> cursorAllocator, ExecutorService executorService, AtomicInteger processing ) {
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
            try (InternalTransaction tx = db.beginTransaction(KernelTransaction.Type.EXPLICIT, LoginContext.AUTH_DISABLED)) {
                KernelTransaction ktx = tx.kernelTransaction();
                try (C cursor = cursorAllocator.apply( ktx )) {
                    if (scan.reserveBatch( cursor, batchSize, ktx.cursorContext(), AccessMode.Static.FULL )) {
                        // Branch out so that all available threads will get saturated
                        executorService.submit( new BatchJob<>( scan, batchSize, db, consumer, result, cursorAllocator, executorService, processing ) );
                        executorService.submit( new BatchJob<>( scan, batchSize, db, consumer, result, cursorAllocator, executorService, processing ) );
                        while (processAndReport(ktx, cursor)) {
                            // just continue processing...
                        }
                    }
                }
                tx.commit();
                return null;
            } finally {
                result.batches.incrementAndGet();
                processing.decrementAndGet();
            }
        }

        private boolean processAndReport(KernelTransaction ktx, C cursor) {
            if (cursor.next()) {
                try {
                    consumer.accept(ktx, cursor);
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
