package apoc.index;

import java.util.Iterator;
import java.util.concurrent.BlockingQueue;

/**
 *  @author Stefan Armbruster
 *  @since 18.01.2019
 *
 *  provides an iterator over a {@link BlockingQueue} featuring {@link BlockingQueue::take} to build an iterator.
 *  If the taken value equals a defined "poison" value, the iterator is considered exhausted.
 */
public class BlockingQueuePoisonIterator<E> implements Iterator<E> {

    private final E poison;
    private final BlockingQueue<E> queue;
    private E nextElement;
    private boolean poisoned = false;
    private int count = 0;

    public BlockingQueuePoisonIterator(BlockingQueue<E> queue, E poison) {
        this.queue = queue;
        this.poison = poison;
    }

    @Override
    public boolean hasNext() {
        if (poisoned) {
            return false;
        } else {
            if (nextElement == null) {
                try {
                    nextElement = queue.take();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                if (nextElement.equals(poison)) {
                    poisoned = true;
                    return false;
                } else {
                    return true;
                }
            } else {
                return true;
            }
        }
    }

    @Override
    public E next() {
        if (poisoned) {
            throw new IllegalStateException("already poisoned");
        }
        E ret = nextElement;
        nextElement = null;
//        System.out.println("HURZ " + count++ + " " + ret);
        return ret;
    }
}
