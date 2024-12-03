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

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Abstract class for how you usually implement iterators when you don't know
 * how many objects there are (which is pretty much every time)
 *
 * Basically the {@link #hasNext()} method will look up the next object and
 * cache it. The cached object is then set to {@code null} in {@link #next()}.
 * So you only have to implement one method, {@code fetchNextOrNull} which
 * returns {@code null} when the iteration has reached the end, and you're done.
 */
public abstract class PrefetchingIteratorExtended<T> implements Iterator<T> {
    private boolean hasFetchedNext;
    private T nextObject;

    /**
     * @return {@code true} if there is a next item to be returned from the next
     * call to {@link #next()}.
     */
    @Override
    public boolean hasNext() {
        return peek() != null;
    }

    /**
     * @return the next element that will be returned from {@link #next()} without
     * actually advancing the iterator
     */
    public T peek() {
        if (hasFetchedNext) {
            return nextObject;
        }

        nextObject = fetchNextOrNull();
        hasFetchedNext = true;
        return nextObject;
    }

    /**
     * Uses {@link #hasNext()} to try to fetch the next item and returns it
     * if found, otherwise it throws a {@link NoSuchElementException}.
     *
     * @return the next item in the iteration, or throws
     * {@link NoSuchElementException} if there's no more items to return.
     */
    @Override
    public T next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        T result = nextObject;
        nextObject = null;
        hasFetchedNext = false;
        return result;
    }

    protected abstract T fetchNextOrNull();

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
