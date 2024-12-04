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

class WrappingResourceIteratorExtended<T> extends PrefetchingResourceIteratorExtended {
    private final Iterator<T> iterator;

    WrappingResourceIteratorExtended(Iterator<T> iterator) {
        this.iterator = iterator;
    }

    @Override
    public void close() {
        // nothing to release
    }

    @Override
    public void remove() {
        iterator.remove();
    }

    @Override
    protected T fetchNextOrNull() {
        return iterator.hasNext() ? iterator.next() : null;
    }
}
