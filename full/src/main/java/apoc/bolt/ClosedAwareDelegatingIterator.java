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
package apoc.bolt;

import org.neo4j.driver.exceptions.ResultConsumedException;

import java.util.Iterator;
import java.util.function.Consumer;

/**
 * if we would pass through a bolt result stream directly as a procedure result stream, there's an issue regarding
 * hasNext() throwing an exception.
 * This wrapper class catches the exception and deals with it gracefully
 * @param <T>
 */
public class ClosedAwareDelegatingIterator<T> implements Iterator<T> {
    private final Iterator<T> delegate;
    private boolean closed = false;

    public ClosedAwareDelegatingIterator(Iterator<T> delegate) {
        this.delegate = delegate;
    }

    @Override
    public boolean hasNext() {
        if (closed) {
            return false;
        }
        try {
            return delegate.hasNext();
        } catch (ResultConsumedException e) {
            closed = true;
            return false;
        }
    }

    @Override
    public T next() {
        try {

            return delegate.next();
        } catch (Exception e) {
            throw e;
        }
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void forEachRemaining(Consumer<? super T> action) {
        throw new UnsupportedOperationException();
    }
}
