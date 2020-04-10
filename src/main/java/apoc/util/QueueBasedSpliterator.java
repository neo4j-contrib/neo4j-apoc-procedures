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
    private volatile boolean foundTombstone = false;
    private final int timeoutSeconds;

    public QueueBasedSpliterator(BlockingQueue<T> queue, T tombstone, TerminationGuard terminationGuard, int timeoutSeconds) {
        this.queue = queue;
        this.tombstone = tombstone;
        this.terminationGuard = terminationGuard;
        this.timeoutSeconds = timeoutSeconds;
    }

    @Override
    public boolean tryAdvance(Consumer<? super T> action) {
        if (foundTombstone) return false;
        terminationGuard.check();
        T element = QueueUtil.take(queue, timeoutSeconds, () -> terminationGuard.check());
        if (element.equals(tombstone)) {
            foundTombstone = true;
            return false;
        } else {
            action.accept(element);
            return true;
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
