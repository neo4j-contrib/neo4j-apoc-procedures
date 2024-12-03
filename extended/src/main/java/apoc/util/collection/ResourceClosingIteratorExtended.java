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

import org.neo4j.graphdb.Resource;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.ResourceUtils;

import java.util.Iterator;
import java.util.NoSuchElementException;

public abstract class ResourceClosingIteratorExtended<T, V> implements ResourceIterator<V> {
    public static <R> ResourceIterator<R> newResourceIterator(Iterator<R> iterator, Resource... resources) {
        return new ResourceClosingIteratorExtended<>(iterator, resources) {
            @Override
            public R map(R elem) {
                return elem;
            }
        };
    }

    /**
     * Return a {@link ResourceIterator} for the provided {@code iterable} that will also close
     * this {@code iterable} when the returned iterator is itself closed. Please note, it is
     * <b>much</b> preferred to explicitly close the {@link ResourceIterable} but this utility
     * provides a way of cleaning up resources when the {@code iterable} is never exposed to
     * client code; for example when the {@link ResourceIterator} is the return-type of a method
     * call.
     *
     * @param iterable the iterable to provider the iterator
     * @param <R> the type of elements in the given iterable
     * @return the iterator for the provided {@code iterable}
     */
    public static <R> ResourceIterator<R> fromResourceIterable(ResourceIterable<R> iterable) {
        ResourceIterator<R> iterator = iterable.iterator();
        return newResourceIterator(iterator, iterator, iterable);
    }

    private Resource[] resources;
    private final Iterator<T> iterator;

    ResourceClosingIteratorExtended(Iterator<T> iterator, Resource... resources) {
        this.resources = resources;
        this.iterator = iterator;
    }

    @Override
    public void close() {
        if (resources != null) {
            ResourceUtils.closeAll(resources);
            resources = null;
        }
    }

    @Override
    public boolean hasNext() {
        boolean hasNext = iterator.hasNext();
        if (!hasNext) {
            close();
        }
        return hasNext;
    }

    public abstract V map(T elem);

    @Override
    public V next() {
        try {
            return map(iterator.next());
        } catch (NoSuchElementException e) {
            close();
            throw e;
        }
    }

    @Override
    public void remove() {
        iterator.remove();
    }
}
