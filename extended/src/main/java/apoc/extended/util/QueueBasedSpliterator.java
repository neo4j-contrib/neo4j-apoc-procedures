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
package apoc.extended.util;

import java.util.Spliterator;
import java.util.concurrent.BlockingQueue;
import java.util.function.Consumer;
import org.neo4j.procedure.TerminationGuard;

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
    private final Runnable onTombstone;

    public QueueBasedSpliterator(
            BlockingQueue<T> queue, T tombstone, TerminationGuard terminationGuard, int timeoutSeconds) {
        this(queue, tombstone, terminationGuard, timeoutSeconds, () -> {});
    }

    /**
     * @param onTombstone run once, on the consuming thread, when the tombstone is observed. The producer enqueues the
     *                    tombstone even when it failed, so this is the hook for reporting that failure to the consumer
     *                    instead of letting the stream end as if it had completed normally.
     */
    public QueueBasedSpliterator(
            BlockingQueue<T> queue,
            T tombstone,
            TerminationGuard terminationGuard,
            int timeoutSeconds,
            Runnable onTombstone) {
        this.queue = queue;
        this.tombstone = tombstone;
        this.terminationGuard = terminationGuard;
        this.timeoutSeconds = timeoutSeconds;
        this.onTombstone = onTombstone;
    }

    @Override
    public boolean tryAdvance(Consumer<? super T> action) {
        if (foundTombstone) return false;
        terminationGuard.check();
        T element = QueueUtil.take(queue, timeoutSeconds, () -> terminationGuard.check());
        if (element.equals(tombstone)) {
            foundTombstone = true;
            onTombstone.run();
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
