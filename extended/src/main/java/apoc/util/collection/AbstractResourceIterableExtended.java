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
package apoc.util.collection;

import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;

import java.util.Arrays;
import java.util.BitSet;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;

public abstract class AbstractResourceIterableExtended<T> implements ResourceIterable<T> {
    // start with 2 as most cases will only generate a single iterator but gives a little leeway to save on expansions
    private TrackingResourceIterator<?>[] trackedIterators = new TrackingResourceIterator<?>[2];

    private BitSet trackedIteratorsInUse = new BitSet();

    private boolean closed;

    protected abstract ResourceIterator<T> newIterator();

    @Override
    public final ResourceIterator<T> iterator() {
        if (closed) {
            throw new ResourceIteratorCloseFailedExceptionExtended(
                    ResourceIterable.class.getSimpleName() + " has already been closed");
        }

        return new TrackingResourceIterator<>(Objects.requireNonNull(newIterator()), this::register, this::unregister);
    }

    @Override
    public final void close() {
        if (!closed) {
            try {
                internalClose();
            } finally {
                closed = true;
                onClosed();
            }
        }
    }

    /**
     * Callback method that allows subclasses to perform their own specific closing logic
     */
    protected void onClosed() {}

    private void register(TrackingResourceIterator<?> iterator) {
        if (trackedIteratorsInUse.cardinality() == trackedIterators.length) {
            trackedIterators = Arrays.copyOf(trackedIterators, trackedIterators.length << 1);
        }

        final var freeIndex = trackedIteratorsInUse.nextClearBit(0);
        trackedIterators[freeIndex] = iterator;
        trackedIteratorsInUse.set(freeIndex);
    }

    private void unregister(TrackingResourceIterator<?> iterator) {
        final var lastSetBit = trackedIteratorsInUse.previousSetBit(trackedIterators.length);
        for (int i = 0; i <= lastSetBit; i++) {
            if (trackedIterators[i] == iterator) {
                trackedIterators[i] = null;
                trackedIteratorsInUse.clear(i);
                break;
            }
        }
    }

    private void internalClose() {
        ResourceIteratorCloseFailedExceptionExtended closeThrowable = null;
        final var lastSetBit = trackedIteratorsInUse.previousSetBit(trackedIterators.length);
        for (int i = 0; i <= lastSetBit; i++) {
            if (trackedIterators[i] == null) {
                continue;
            }

            try {
                trackedIterators[i].internalClose();
            } catch (Exception e) {
                if (closeThrowable == null) {
                    closeThrowable =
                            new ResourceIteratorCloseFailedExceptionExtended("Exception closing a resource iterator.", e);
                } else {
                    closeThrowable.addSuppressed(e);
                }
            }
        }

        trackedIterators = null;
        trackedIteratorsInUse = null;

        if (closeThrowable != null) {
            throw closeThrowable;
        }
    }

    private static final class TrackingResourceIterator<T> implements ResourceIterator<T> {
        private final ResourceIterator<T> delegate;
        private final Consumer<TrackingResourceIterator<?>> registerCallback;
        private final Consumer<TrackingResourceIterator<?>> unregisterCallback;

        private boolean closed;

        private TrackingResourceIterator(
                ResourceIterator<T> delegate,
                Consumer<TrackingResourceIterator<?>> registerCallback,
                Consumer<TrackingResourceIterator<?>> unregisterCallback) {
            this.delegate = delegate;
            this.registerCallback = registerCallback;
            this.unregisterCallback = unregisterCallback;

            registerCallback.accept(this);
        }

        @Override
        public boolean hasNext() {
            boolean hasNext = delegate.hasNext();
            if (!hasNext) {
                close();
            }
            return hasNext;
        }

        @Override
        public T next() {
            return delegate.next();
        }

        @Override
        public <R> ResourceIterator<R> map(Function<T, R> map) {
            return new TrackingResourceIterator<>(
                    ResourceIterator.super.map(map), registerCallback, unregisterCallback);
        }

        @Override
        public void close() {
            if (!closed) {
                internalClose();
                unregisterCallback.accept(this);
            }
        }

        private void internalClose() {
            try {
                delegate.close();
            } finally {
                closed = true;
            }
        }
    }
}
