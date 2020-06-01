package apoc.bolt;

import org.neo4j.driver.exceptions.ResultConsumedException;

import java.util.Iterator;
import java.util.function.Consumer;

/**
 * if we would pass through a bolt result stream directly as a procedure result stream, there's an issue regarding
 * hasNext() throwing an exception.
 * This wrapper class catches the exception and deals with it gracefully
 * @param <T>
 */
public class ClosedAwareDelegatingIterator<T> implements Iterator<T> {
    private final Iterator<T> delegate;
    private boolean closed = false;

    public ClosedAwareDelegatingIterator(Iterator<T> delegate) {
        this.delegate = delegate;
    }

    @Override
    public boolean hasNext() {
        if (closed) {
            return false;
        }
        try {
            return delegate.hasNext();
        } catch (ResultConsumedException e) {
            closed = true;
            return false;
        }
    }

    @Override
    public T next() {
        try {

            return delegate.next();
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void forEachRemaining(Consumer<? super T> action) {
        throw new UnsupportedOperationException();
    }
}
