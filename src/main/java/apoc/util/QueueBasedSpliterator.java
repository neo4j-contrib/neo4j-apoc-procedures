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
    private boolean foundTombstone = false;

    public QueueBasedSpliterator(BlockingQueue<T> queue, T tombstone, TerminationGuard terminationGuard) {
        this.queue = queue;
        this.tombstone = tombstone;
        this.terminationGuard = terminationGuard;
    }

    @Override
    public boolean tryAdvance(Consumer<? super T> action) {
        if (Util.transactionIsTerminated(terminationGuard) || foundTombstone) return false;
        try {
            T element = queue.take();
            if (element.equals(tombstone)) {
                foundTombstone = true;
            } else {
                action.accept(element);
            }
            return true;
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
