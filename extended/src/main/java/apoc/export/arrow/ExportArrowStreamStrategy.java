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
package apoc.export.arrow;

import apoc.result.ByteArrayResultExtended;
import apoc.util.UtilExtended;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.ipc.ArrowWriter;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static apoc.export.arrow.ExportArrowFileStrategy.writeJsonResult;

public interface ExportArrowStreamStrategy<IN> extends ExportArrowStrategy<IN, Stream<ByteArrayResultExtended>> {

    Iterator<Map<String, Object>> toIterator(IN data);

    default byte[] writeBatch(BufferAllocator bufferAllocator, List<Map<String, Object>> rows) {
        try (final VectorSchemaRoot root = VectorSchemaRoot.create(schemaFor(rows), bufferAllocator);
                final ByteArrayOutputStream out = new ByteArrayOutputStream();
                final ArrowWriter writer = newArrowWriter(root, out)) {
            AtomicInteger counter = new AtomicInteger();
            root.allocateNew();
            rows.forEach(row -> {
                final int index = counter.getAndIncrement();
                root.getFieldVectors().forEach(fe -> {
                    Object value = convertValue(row.get(fe.getName()));
                    write(index, value, fe);
                });
            });
            root.setRowCount(counter.get());
            writer.writeBatch();
            root.clear();
            return out.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    default Stream<ByteArrayResultExtended> export(IN data, ArrowConfig config) {
        class ExportIterator implements Iterator<ByteArrayResultExtended> {
            ByteArrayResultExtended current;
            int batchCount = 0;
            Iterator<Map<String, Object>> it;

            public ExportIterator(IN data) {
                it = toIterator(data);
                current = null;
                computeBatch();
            }

            @Override
            public boolean hasNext() {
                return current != null;
            }

            @Override
            public ByteArrayResultExtended next() {
                ByteArrayResultExtended result = current;
                current = null;
                computeBatch();
                return result;
            }

            private void computeBatch() {
                boolean keepIterating = true;
                List<Map<String, Object>> rows = new ArrayList<>(config.getBatchSize());

                while (!UtilExtended.transactionIsTerminated(getTerminationGuard()) && it.hasNext() && keepIterating) {
                    rows.add(it.next());
                    if (batchCount > 0 && batchCount % config.getBatchSize() == 0) {
                        final byte[] bytes = writeBatch(getBufferAllocator(), rows);
                        current = new ByteArrayResultExtended(bytes);
                        keepIterating = false;
                    }
                    ++batchCount;
                }

                if (!rows.isEmpty()) {
                    final byte[] bytes = writeBatch(getBufferAllocator(), rows);
                    current = new ByteArrayResultExtended(bytes);
                }
            }
        }

        var streamIterator = new ExportIterator(data);
        Iterable<ByteArrayResultExtended> iterable = () -> streamIterator;
        return StreamSupport.stream(iterable.spliterator(), false);
    }

    default Object convertValue(Object data) {
        return data == null ? null : writeJsonResult(data);
    }

    default ArrowWriter newArrowWriter(VectorSchemaRoot root, OutputStream out) {
        return new ArrowStreamWriter(root, new DictionaryProvider.MapDictionaryProvider(), Channels.newChannel(out));
    }

    Schema schemaFor(List<Map<String, Object>> rows);
}
