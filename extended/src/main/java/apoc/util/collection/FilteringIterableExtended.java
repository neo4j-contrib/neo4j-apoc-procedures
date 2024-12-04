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
import java.util.function.Predicate;

/**
 * An iterable which filters another iterable, only letting items with certain
 * criteria pass through. All iteration/filtering is done lazily.
 *
 * @param <T> the type of items in the iteration.
 */
public class FilteringIterableExtended<T> implements Iterable<T> {
    private final Iterable<T> source;
    private final Predicate<T> predicate;

    public FilteringIterableExtended(Iterable<T> source, Predicate<T> predicate) {
        this.source = source;
        this.predicate = predicate;
    }

    @Override
    public Iterator<T> iterator() {
        return new FilteringIteratorExtended<>(source.iterator(), predicate);
    }
}
