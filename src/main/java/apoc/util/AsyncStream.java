package apoc.util;

import java.util.Iterator;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.Spliterators.spliteratorUnknownSize;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public final class AsyncStream<T> implements Consumer<T>, Iterator<T> {
    public static <T> Stream<T> async(Executor executor, String taskName, Consumer<Consumer<T>> body) {
        AsyncStream<T> sink = new AsyncStream<>(16);
        executor.execute(() -> {
            Thread thread = Thread.currentThread();
            String name = thread.getName();
            thread.setName(taskName);
            try {
                body.accept(sink);
            } finally {
                thread.setName(name);
                sink.done();
            }
        });
        return StreamSupport.stream(spliteratorUnknownSize(sink, 0), false);
    }

    private final BlockingQueue<T> queue;
    private final CountDownLatch done = new CountDownLatch(1);

    private AsyncStream(int capacity) {
        this.queue = new ArrayBlockingQueue<>(capacity);
    }

    @Override
    public boolean hasNext() {
        if (queue.isEmpty()) {
            try {
                for (long timeout = 10; !done.await(timeout, MILLISECONDS); timeout = timeout > 1000 ? timeout : timeout * 2) {
                    if (!queue.isEmpty()) {
                        return true;
                    }
                }
                return !queue.isEmpty();
            } catch (InterruptedException e) {
                throw new IllegalStateException(e);
            }
        } else {
            return true;
        }
    }

    @Override
    public T next() {
        return queue.remove();
    }

    @Override
    public void accept(T t) {
        try {
            queue.put(t);
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }

    private void done() {
        done.countDown();
    }
}
