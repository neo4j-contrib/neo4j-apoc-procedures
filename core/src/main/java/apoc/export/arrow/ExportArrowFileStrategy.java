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

import apoc.convert.Json;
import apoc.export.util.ProgressReporter;
import apoc.result.ProgressInfo;
import apoc.util.FileUtils;
import apoc.util.QueueBasedSpliterator;
import apoc.util.QueueUtil;
import apoc.util.Util;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.ArrowWriter;
import org.apache.arrow.vector.types.pojo.Schema;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.logging.Log;
import org.neo4j.procedure.TerminationGuard;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public interface ExportArrowFileStrategy<IN> extends ExportArrowStrategy<IN, Stream<ProgressInfo>> {

    Iterator<Map<String, Object>> toIterator(ProgressReporter reporter, IN data);

    default Stream<ProgressInfo> export(IN data, ArrowConfig config) {
        final BlockingQueue<ProgressInfo> queue = new ArrayBlockingQueue<>(10);
        final OutputStream out = FileUtils.getOutputStream(getFileName());
        ProgressInfo progressInfo = new ProgressInfo(getFileName(), getSource(data), "arrow");
        progressInfo.batchSize = config.getBatchSize();
        ProgressReporter reporter = new ProgressReporter(null, null, progressInfo);
        Util.inTxFuture(getExecutorService(), getGraphDatabaseApi(), txInThread -> {
            int batchCount = 0;
            List<Map<String, Object>> rows = new ArrayList<>(config.getBatchSize());
            VectorSchemaRoot root = null;
            ArrowWriter writer = null;
            try {
                Iterator<Map<String, Object>> it = toIterator(reporter, data);
                while (!Util.transactionIsTerminated(getTerminationGuard()) && it.hasNext()) {
                    rows.add(it.next());
                    if (batchCount > 0 && batchCount % config.getBatchSize() == 0) {
                        if (root == null) {
                            root = VectorSchemaRoot.create(schemaFor(rows), getBufferAllocator());
                            writer = newArrowWriter(root, out);
                        }
                        writeBatch(root, writer, rows);
                        rows.clear();
                    }
                    ++batchCount;
                }
                if (!rows.isEmpty()) {
                    if (root == null) {
                        root = VectorSchemaRoot.create(schemaFor(rows), getBufferAllocator());
                        writer = newArrowWriter(root, out);
                    }
                    writeBatch(root, writer, rows);
                }
                QueueUtil.put(queue, progressInfo, 10);
            } catch (Exception e) {
                getLogger().error("Exception while extracting Arrow data:", e);
            } finally {
                reporter.done();
                Util.close(root);
                Util.close(writer);
                QueueUtil.put(queue, ProgressInfo.EMPTY, 10);
            }
            return true;
        });
        QueueBasedSpliterator<ProgressInfo> spliterator = new QueueBasedSpliterator<>(queue, ProgressInfo.EMPTY, getTerminationGuard(), Integer.MAX_VALUE);
        return StreamSupport.stream(spliterator, false);
    }

    String getSource(IN data);


    default void writeBatch(VectorSchemaRoot root, ArrowWriter writer, List<Map<String, Object>> rows) {
        AtomicInteger counter = new AtomicInteger();
        root.allocateNew();
        rows.forEach(row -> {
            final int index = counter.getAndIncrement();
            root.getFieldVectors()
                    .forEach(fe -> {
                        Object value = convertValue(row.get(fe.getName()));
                        write(index, value, fe);
                    });
        });
        root.setRowCount(counter.get());
        try {
            writer.writeBatch();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        root.clear();
    }

    String getFileName();

    TerminationGuard getTerminationGuard();

    BufferAllocator getBufferAllocator();

    GraphDatabaseService getGraphDatabaseApi();

    ExecutorService getExecutorService();

    Log getLogger();

    default Object convertValue(Object data) {
        return data == null ? null : Json.writeJsonResult(data);
    }

    default ArrowWriter newArrowWriter(VectorSchemaRoot root, OutputStream out) {
        return new ArrowFileWriter(root, new DictionaryProvider.MapDictionaryProvider(), Channels.newChannel(out));
    }

    Schema schemaFor(List<Map<String, Object>> rows);
}