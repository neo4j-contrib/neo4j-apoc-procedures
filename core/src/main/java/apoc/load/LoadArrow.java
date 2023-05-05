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
package apoc.load;

import apoc.Pools;
import apoc.result.MapResult;
import apoc.util.FileUtils;
import apoc.util.JsonUtil;
import apoc.util.Util;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.util.Text;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.TerminationGuard;
import org.neo4j.values.storable.Values;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class LoadArrow {

    @Context
    public GraphDatabaseService db;

    @Context
    public Pools pools;

    @Context
    public Log log;

    @Context
    public TerminationGuard terminationGuard;

    private static class ArrowSpliterator extends Spliterators.AbstractSpliterator<MapResult> {

        private final ArrowReader reader;
        private final VectorSchemaRoot schemaRoot;
        private final AtomicInteger counter;

        public ArrowSpliterator(ArrowReader reader, VectorSchemaRoot schemaRoot) throws IOException {
            super(Long.MAX_VALUE, Spliterator.ORDERED);
            this.reader = reader;
            this.schemaRoot = schemaRoot;
            this.counter = new AtomicInteger();
            this.reader.loadNextBatch();
        }

        @Override
        public synchronized boolean tryAdvance(Consumer<? super MapResult> action) {
            try {
                if (counter.get() >= schemaRoot.getRowCount()) {
                    if (reader.loadNextBatch()) {
                        counter.set(0);
                    } else {
                        return false;
                    }
                }
                final Map<String, Object> row = schemaRoot.getFieldVectors()
                        .stream()
                        .collect(HashMap::new, (map, fieldVector) -> map.put(fieldVector.getName(), read(fieldVector, counter.get())), HashMap::putAll); // please look at https://bugs.openjdk.java.net/browse/JDK-8148463
                counter.incrementAndGet();
                action.accept(new MapResult(row));
                return true;
            } catch (Exception e) {
                return false;
            }

        }
    }

    @Procedure(name = "apoc.load.arrow.stream")
    @Description("apoc.load.arrow.stream(source, config) - imports nodes and relationships from the provided byte[]")
    public Stream<MapResult> stream(
            @Name("source") byte[] source,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws IOException {
        RootAllocator allocator = new RootAllocator();
        ByteArrayInputStream inputStream = new ByteArrayInputStream(source);
        ArrowStreamReader streamReader = new ArrowStreamReader(inputStream, allocator);
        VectorSchemaRoot schemaRoot = streamReader.getVectorSchemaRoot();
        return StreamSupport.stream(new ArrowSpliterator(streamReader, schemaRoot), false)
                .onClose(() -> {
                    Util.close(allocator);
                    Util.close(streamReader);
                    Util.close(schemaRoot);
                    Util.close(inputStream);
                });
    }

    @Procedure(name = "apoc.load.arrow")
    @Description("apoc.load.arrow(fileName, config) - imports nodes and relationships from the provided file")
    public Stream<MapResult> file(
            @Name("source") String fileName,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws IOException {
        final SeekableByteChannel channel = FileUtils.inputStreamFor(fileName, null, null, null)
                .asChannel();
        RootAllocator allocator = new RootAllocator();
        ArrowFileReader streamReader = new ArrowFileReader(channel, allocator);
        VectorSchemaRoot schemaRoot = streamReader.getVectorSchemaRoot();
        return StreamSupport.stream(new ArrowSpliterator(streamReader, schemaRoot), false)
                .onClose(() -> {
                    Util.close(allocator);
                    Util.close(streamReader);
                    Util.close(schemaRoot);
                    Util.close(channel);
                });
    }

    private static Object read(FieldVector fieldVector, int index) {
        if (fieldVector.isNull(index)) {
            return null;
        } else if (fieldVector instanceof DateMilliVector) {
            DateMilliVector fe = (DateMilliVector) fieldVector;
            return Instant.ofEpochMilli(fe.get(index)).atOffset(ZoneOffset.UTC);
        } else if (fieldVector instanceof BitVector) {
            BitVector fe = (BitVector) fieldVector;
            return fe.get(index) == 1;
        } else {
            Object object = fieldVector.getObject(index);
            return getObject(object);
        }
    }

    private static Object getObject(Object object) {
        if (object instanceof Collection) {
            return ((Collection<?>) object).stream()
                    .map(LoadArrow::getObject)
                    .collect(Collectors.toList());
        }
        if (object instanceof Map) {
            return ((Map<String, Object>) object).entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> getObject(e.getValue())));
        }
        if (object instanceof Text) {
            return object.toString();
        }
        try {
            // we test if is a valid Neo4j type
            return Values.of(object);
        } catch (Exception e) {
            // otherwise we try coerce it
            return valueToString(object);
        }
    }

    private static String valueToString(Object value) {
        return JsonUtil.writeValueAsString(value);
    }

}