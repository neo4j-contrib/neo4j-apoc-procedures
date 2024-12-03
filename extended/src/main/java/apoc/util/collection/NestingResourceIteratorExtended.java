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

import org.neo4j.graphdb.ResourceIterator;

import java.util.Iterator;

public abstract class NestingResourceIteratorExtended<T, U> extends PrefetchingResourceIteratorExtended<T> {
    private final Iterator<U> source;
    private ResourceIterator<T> currentNestedIterator;

    protected NestingResourceIteratorExtended(Iterator<U> source) {
        this.source = source;
    }

    protected abstract ResourceIterator<T> createNestedIterator(U item);

    @Override
    protected T fetchNextOrNull() {
        if (currentNestedIterator == null || !currentNestedIterator.hasNext()) {
            while (source.hasNext()) {
                U currentSurfaceItem = source.next();
                close();
                currentNestedIterator = createNestedIterator(currentSurfaceItem);
                if (currentNestedIterator.hasNext()) {
                    break;
                }
            }
        }
        return currentNestedIterator != null && currentNestedIterator.hasNext() ? currentNestedIterator.next() : null;
    }

    @Override
    public void close() {
        if (currentNestedIterator != null) {
            currentNestedIterator.close();
        }
    }
}
