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
package apoc.index;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

/**
 * Populate a {@link BlockingQueue} with a poison element after a stream iteration has been finished.
 * Defacto a noop collector expect adding the poison in the finisher.
 * @param <E>
 */
public class QueuePoisoningCollector<E> implements Collector<Void, Void, Void> {
    private final BlockingQueue<E> queue;
    private final E poison;

    public QueuePoisoningCollector(BlockingQueue<E> queue, E poison) {
        this.queue = queue;
        this.poison = poison;
    }

    @Override
    public Supplier<Void> supplier() {
        return () -> null;
    }

    @Override
    public BiConsumer<Void, Void> accumulator() {
        return (integer, integer2) -> {
        };
    }

    @Override
    public BinaryOperator<Void> combiner() {
        return null;
    }

    @Override
    public Function<Void, Void> finisher() {
        return integer -> {
            try {
                queue.put(poison);
                return null;
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        };
    }

    @Override
    public Set<Characteristics> characteristics() {
        Set<Characteristics> characteristics = new HashSet<>();
        characteristics.add(Characteristics.UNORDERED);
        characteristics.add(Characteristics.CONCURRENT);
        return characteristics;
    }
}
