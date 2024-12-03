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

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Utility methods for processing iterables. Where possible, If the iterable implements
 * {@link Resource}, it will be {@link Resource#close() closed} when the processing
 * has been completed.
 */
public final class IterablesExtended {

    /**
     * Collect all the elements available in {@code iterable} and add them to the
     * provided {@code collection}.
     * <p>
     * If the {@code iterable} implements {@link Resource} it will be
     * {@link Resource#close() closed} in a {@code finally} block after all
     * the items have been added.
     *
     * @param collection the collection to add items to.
     * @param iterable the iterable from which items will be collected
     * @param <T> the type of elements in {@code iterable}.
     * @param <C> the type of the collection to add the items to.
     * @return the {@code collection} that has been updated.
     */
    public static <T, C extends Collection<T>> C addAll(C collection, Iterable<? extends T> iterable) {
        try {
            Iterator<? extends T> iterator = iterable.iterator();
            try {
                while (iterator.hasNext()) {
                    collection.add(iterator.next());
                }
            } finally {
                IteratorsExtended.tryCloseResource(iterator);
            }
        } finally {
            tryCloseResource(iterable);
        }

        return collection;
    }

    @SafeVarargs
    public static <T, C extends T> Iterable<T> iterable(C... items) {
        return Arrays.asList(items);
    }

    @SuppressWarnings("unchecked")
    public static <T> T[] asArray(Class<T> componentType, Iterable<T> iterable) {
        if (iterable == null) {
            return null;
        }

        List<T> list = asList(iterable);
        return list.toArray((T[]) Array.newInstance(componentType, list.size()));
    }

    public static <T> ResourceIterable<T> asResourceIterable(final Iterable<T> iterable) {
        if (iterable instanceof ResourceIterable<?>) {
            return (ResourceIterable<T>) iterable;
        }
        return new AbstractResourceIterableExtended<>() {
            @Override
            protected ResourceIterator<T> newIterator() {
                return IteratorsExtended.asResourceIterator(iterable.iterator());
            }

            @Override
            protected void onClosed() {
                tryCloseResource(iterable);
            }
        };
    }

    /**
     * Returns the given iterable's first element or {@code null} if no
     * element found.
     * <p>
     * If the {@code iterable} implements {@link Resource}, then it will be closed in a {@code finally} block
     * after the first item has been retrieved, or failed to be retrieved.
     * <p>
     * If the {@link Iterable#iterator() iterator} created by the {@code iterable} implements {@link Resource}
     * it will be {@link Resource#close() closed} in a {@code finally} block after the single item
     * has been retrieved, or failed to be retrieved.
     *
     * @param <T> the type of elements in {@code iterable}.
     * @param iterable the {@link Iterable} to get elements from.
     * @return the first element in the {@code iterable}, or {@code null} if no
     * element found.
     */
    public static <T> T firstOrNull(Iterable<T> iterable) {
        try {
            return IteratorsExtended.firstOrNull(iterable.iterator());
        } finally {
            tryCloseResource(iterable);
        }
    }

    /**
     * Returns the given iterable's first element. If no element is found a
     * {@link NoSuchElementException} is thrown.
     * <p>
     * If the {@code iterable} implements {@link Resource}, then it will be closed in a {@code finally} block
     * after the first item has been retrieved, or failed to be retrieved.
     *
     * @param <T> the type of elements in {@code iterable}.
     * @param iterable the {@link Iterable} to get elements from.
     * @return the first element in the {@code iterable}.
     * @throws NoSuchElementException if no element found.
     */
    public static <T> T first(Iterable<T> iterable) {
        try {
            return IteratorsExtended.first(iterable.iterator());
        } finally {
            tryCloseResource(iterable);
        }
    }

    /**
     * Returns the given iterable's single element. If there are no elements
     * or more than one element in the iterable a {@link NoSuchElementException}
     * will be thrown.
     * <p>
     * If the {@code iterable} implements {@link Resource}, then it will be closed in a {@code finally} block
     * after the single item has been retrieved, or failed to be retrieved.
     * <p>
     * If the {@link Iterable#iterator() iterator} created by the {@code iterable} implements {@link Resource}
     * it will be {@link Resource#close() closed} in a {@code finally} block after the single item
     * has been retrieved, or failed to be retrieved.
     *
     * @param <T> the type of elements in {@code iterable}.
     * @param iterable the {@link Iterable} to get elements from.
     * @return the single element in the {@code iterable}.
     * @throws NoSuchElementException if there isn't exactly one element.
     */
    public static <T> T single(Iterable<T> iterable) {
        try {
            return IteratorsExtended.single(iterable.iterator());
        } finally {
            tryCloseResource(iterable);
        }
    }

    /**
     * Counts the number of items in the {@code iterable} by looping through it.
     * <p>
     * If the {@code iterable} implements {@link Resource}, then it will be closed in a {@code finally} block
     * after all its items have been counted.
     * <p>
     * If the {@link Iterable#iterator() iterator} created by the {@code iterable} implements {@link Resource}
     * it will be {@link Resource#close() closed} in a {@code finally} block after the items have been counted.
     *
     * @param <T> the type of items in the iterator.
     * @param iterable the {@link Iterable} to count items in.
     * @return the number of items found in {@code iterable}.
     */
    public static <T> long count(Iterable<T> iterable) {
        try {
            return IteratorsExtended.count(iterable.iterator());
        } finally {
            tryCloseResource(iterable);
        }
    }

    /**
     * Creates a list from an iterable.
     * <p>
     * If the {@code iterable} implements {@link Resource}, then it will be closed in a {@code finally} block
     * after all its items have been added.
     * <p>
     * If the {@link Iterable#iterator() iterator} created by the {@code iterable} implements {@link Resource}
     * it will be {@link Resource#close() closed} in a {@code finally} block after all the items have been added.
     *
     * @param iterable The iterable to create the list from.
     * @param <T> The generic type of both the iterable and the list.
     * @return a list containing all items from the iterable.
     */
    public static <T> List<T> asList(Iterable<T> iterable) {
        return addAll(new ArrayList<>(), iterable);
    }

    /**
     * Creates a {@link Set} from an {@link Iterable}.
     * <p>
     * If the {@code iterable} implements {@link Resource}, then it will be closed in a {@code finally} block
     * after all its items have been added.
     * <p>
     * If the {@link Iterable#iterator() iterator} created by the {@code iterable} implements {@link Resource}
     * it will be {@link Resource#close() closed} in a {@code finally} block after all the items have been added.
     *
     * @param iterable The items to create the set from.
     * @param <T> The generic type of items.
     * @return a set containing all items from the {@link Iterable}.
     */
    public static <T> Set<T> asSet(Iterable<T> iterable) {
        return addAll(new HashSet<>(), iterable);
    }

    /**
     * Create a stream from the given iterable.
     * <p>
     * <b>Note:</b> returned stream needs to be closed via {@link Stream#close()} if the given iterable implements
     * {@link Resource}.
     *
     * @param iterable the iterable to convert to stream
     * @param <T> the type of elements in the given iterable
     * @return stream over the iterable elements
     * @throws NullPointerException when the given iterable is {@code null}
     */
    public static <T> Stream<T> stream(Iterable<T> iterable) {
        Objects.requireNonNull(iterable);
        return IteratorsExtended.stream(iterable.iterator()).onClose(() -> tryCloseResource(iterable));
    }

    /**
     * Close the provided {@code iterable} if it implements {@link Resource}.
     *
     * @param iterable the iterable to check for closing
     */
    private static void tryCloseResource(Iterable<?> iterable) {
        if (iterable instanceof Resource closeable) {
            closeable.close();
        }
    }
}
