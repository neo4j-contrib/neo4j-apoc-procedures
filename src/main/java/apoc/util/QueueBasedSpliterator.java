package apoc.util;

import org.neo4j.procedure.TerminationGuard;

import java.util.Spliterator;
import java.util.concurrent.BlockingQueue;
import java.util.function.Consumer;

/**
 * @author mh
 * @since 06.12.17
 */
public class QueueBasedSpliterator<T> implements Spliterator<T> {
    private final BlockingQueue<T> queue;
    private T tombstone;
    private TerminationGuard terminationGuard;
    private int resultCounter = 0;

    public QueueBasedSpliterator(BlockingQueue<T> queue, T tombstone, TerminationGuard terminationGuard) {
        this.queue = queue;
        this.tombstone = tombstone;
        this.terminationGuard = terminationGuard;
    }

    @Override
    public boolean tryAdvance(Consumer<? super T> action) {
        if (Util.transactionIsTerminated(terminationGuard)) return false;
        try {
            T element = queue.take();
            resultCounter++;
/*
            if (resultCounter % 100 == 0 || element.equals(tombstone)) {
                System.out.println(Thread.currentThread().getName() + " took from queue " + resultCounter);
            }
*/
            if (element.equals(tombstone)) {
                return false;
            } else {
                action.accept(element);
                return true;
            }

        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public Spliterator<T> trySplit() {
        return null;
    }

    public long estimateSize() {
        return Long.MAX_VALUE;
    }

    public int characteristics() {
        return NONNULL;
    }
}
