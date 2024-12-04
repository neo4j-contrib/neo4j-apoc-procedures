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
import org.neo4j.graphdb.ResourceIterator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Contains common functionality regarding {@link Iterator}s and
 * {@link Iterable}s.
 */
public final class IteratorsExtended {

    /**
     * Returns the given iterator's first element or {@code null} if no
     * element found.
     *
     * @param <T> the type of elements in {@code iterator}.
     * @param iterator the {@link Iterator} to get elements from.
     * @return the first element in the {@code iterator}, or {@code null} if no
     * element found.
     */
    public static <T> T firstOrNull(Iterator<T> iterator) {
        try {
            return iterator.hasNext() ? iterator.next() : null;
        } finally {
            tryCloseResource(iterator);
        }
    }

    /**
     * Returns the given iterator's first element. If no element is found a
     * {@link NoSuchElementException} is thrown.
     *
     * @param <T> the type of elements in {@code iterator}.
     * @param iterator the {@link Iterator} to get elements from.
     * @return the first element in the {@code iterator}.
     * @throws NoSuchElementException if no element found.
     */
    public static <T> T first(Iterator<T> iterator) {
        return assertNotNull(iterator, firstOrNull(iterator));
    }

    /**
     * Returns the given iterator's single element or {@code null} if no
     * element found. If there is more than one element in the iterator a
     * {@link NoSuchElementException} will be thrown.
     *
     * If the {@code iterator} implements {@link Resource} it will be {@link Resource#close() closed}
     * in a {@code finally} block after the single item has been retrieved, or failed to be retrieved.
     *
     * @param <T> the type of elements in {@code iterator}.
     * @param iterator the {@link Iterator} to get elements from.
     * @return the single element in {@code iterator}, or {@code null} if no
     * element found.
     * @throws NoSuchElementException if more than one element was found.
     */
    public static <T> T singleOrNull(Iterator<T> iterator) {
        try {
            T result = iterator.hasNext() ? iterator.next() : null;
            if (iterator.hasNext()) {
                throw new NoSuchElementException("More than one element in " + iterator + ". First element is '"
                        + result + "' and the second element is '" + iterator.next() + "'");
            }
            return result;
        } finally {
            tryCloseResource(iterator);
        }
    }

    /**
     * Returns the given iterator's single element. If there are no elements
     * or more than one element in the iterator a {@link NoSuchElementException}
     * will be thrown.
     *
     * If the {@code iterator} implements {@link Resource} it will be {@link Resource#close() closed}
     * in a {@code finally} block after the single item has been retrieved, or failed to be retrieved.
     *
     * @param <T> the type of elements in {@code iterator}.
     * @param iterator the {@link Iterator} to get elements from.
     * @return the single element in the {@code iterator}.
     * @throws NoSuchElementException if there isn't exactly one element.
     */
    public static <T> T single(Iterator<T> iterator) {
        return assertNotNull(iterator, singleOrNull(iterator));
    }

    private static <T> T assertNotNull(Iterator<T> iterator, T result) {
        if (result == null) {
            throw new NoSuchElementException("No element found in " + iterator);
        }
        return result;
    }

    /**
     * Adds all the items in {@code iterator} to {@code collection}.
     * @param <C> the type of {@link Collection} to add to items to.
     * @param <T> the type of items in the collection and iterator.
     * @param iterator the {@link Iterator} to grab the items from.
     * @param collection the {@link Collection} to add the items to.
     * @return the {@code collection} which was passed in, now filled
     * with the items from {@code iterator}.
     */
    private static <C extends Collection<T>, T> C addToCollection(Iterator<T> iterator, C collection) {
        try {
            while (iterator.hasNext()) {
                collection.add(iterator.next());
            }
            return collection;
        } finally {
            tryCloseResource(iterator);
        }
    }

    /**
     * Counts the number of items in the {@code iterator} by looping
     * through it.
     *
     * If the {@code iterator} implements {@link Resource} it will be {@link Resource#close() closed}
     * in a {@code finally} block after the items have been counted.
     *
     * @param <T> the type of items in the iterator.
     * @param iterator the {@link Iterator} to count items in.
     * @return the number of items found in {@code iterator}.
     */
    public static <T> long count(Iterator<T> iterator) {
        try {
            long result = 0;
            while (iterator.hasNext()) {
                iterator.next();
                result++;
            }
            return result;
        } finally {
            tryCloseResource(iterator);
        }
    }

    public static <T> List<T> asList(Iterator<T> iterator) {
        return addToCollection(iterator, new ArrayList<>());
    }

    public static <T> Set<T> asSet(Iterator<T> iterator) {
        return addToCollection(iterator, new HashSet<>());
    }

    /**
     * Creates a {@link Set} from an array of items.an
     *
     * @param items the items to add to the set.
     * @param <T> the type of the items
     * @return the {@link Set} containing the items.
     */
    @SafeVarargs
    public static <T> Set<T> asSet(T... items) {
        return new HashSet<>(Arrays.asList(items));
    }

    public static <T> ResourceIterator<T> asResourceIterator(final Iterator<T> iterator) {
        if (iterator instanceof ResourceIterator<?>) {
            return (ResourceIterator<T>) iterator;
        }
        return new WrappingResourceIteratorExtended<>(iterator);
    }

    /**
     * Create a stream from the given iterator.
     * <p>
     * <b>Note:</b> returned stream needs to be closed via {@link Stream#close()} if the given iterator implements
     * {@link Resource}.
     *
     * @param iterator the iterator to convert to stream
     * @param <T> the type of elements in the given iterator
     * @return stream over the iterator elements
     * @throws NullPointerException when the given stream is {@code null}
     */
    public static <T> Stream<T> stream(Iterator<T> iterator) {
        Objects.requireNonNull(iterator);
        Spliterator<T> spliterator = Spliterators.spliteratorUnknownSize(iterator, 0);
        Stream<T> stream = StreamSupport.stream(spliterator, false);
        if (iterator instanceof Resource resource) {
            return stream.onClose(resource::close);
        }
        return stream;
    }

    /**
     * Close the provided {@code iterator} if it implements {@link Resource}.
     *
     * @param iterator the iterator to check for closing
     */
    public static void tryCloseResource(Iterator<?> iterator) {
        if (iterator instanceof Resource closeable) {
            closeable.close();
        }
    }
}
