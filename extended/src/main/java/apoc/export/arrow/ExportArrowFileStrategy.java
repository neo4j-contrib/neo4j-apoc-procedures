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

import apoc.convert.ConvertUtils;
import apoc.export.util.ProgressReporter;
import apoc.meta.TypesExtended;
import apoc.result.ExportProgressInfoExtended;
import apoc.util.FileUtils;
import apoc.util.Util;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.ArrowWriter;
import org.apache.arrow.vector.types.pojo.Schema;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.logging.Log;
import org.neo4j.procedure.TerminationGuard;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static apoc.util.Util.labelStrings;
import static apoc.util.Util.map;

public interface ExportArrowFileStrategy<IN> extends ExportArrowStrategy<IN, Stream<ExportProgressInfoExtended>> {

    Iterator<Map<String, Object>> toIterator(ProgressReporter reporter, IN data);

    default Stream<ExportProgressInfoExtended> export(IN data, ArrowConfig config) {
        final OutputStream out = FileUtils.getOutputStream(getFileName());
        ExportProgressInfoExtended progressInfo = new ExportProgressInfoExtended(getFileName(), getSource(data), "arrow");
        progressInfo.setBatchSize(config.getBatchSize());
        ProgressReporter reporter = new ProgressReporter(null, null, progressInfo);
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
        } catch (Exception e) {
            getLogger().error("Exception while extracting Arrow data:", e);
        } finally {
            reporter.done();
            Util.close(root);
            Util.close(writer);
        }

        return Stream.of(progressInfo);
    }

    String getSource(IN data);

    default void writeBatch(VectorSchemaRoot root, ArrowWriter writer, List<Map<String, Object>> rows) {
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
        return data == null ? null : writeJsonResult(data);
    }

    default ArrowWriter newArrowWriter(VectorSchemaRoot root, OutputStream out) {
        return new ArrowFileWriter(root, new DictionaryProvider.MapDictionaryProvider(), Channels.newChannel(out));
    }

    Schema schemaFor(List<Map<String, Object>> rows);

    // visible for testing
    public static String NODE = "node";
    public static String RELATIONSHIP = "relationship";

    public static Object writeJsonResult(Object value) {
        TypesExtended type = TypesExtended.of(value);
        switch (type) {
            case NODE:
                return nodeToMap((Node) value);
            case RELATIONSHIP:
                return relToMap((Relationship) value);
            case PATH:
                return writeJsonResult(StreamSupport.stream(((Path) value).spliterator(), false)
                        .map(i -> i instanceof Node ? nodeToMap((Node) i) : relToMap((Relationship) i))
                        .collect(Collectors.toList()));
            case LIST:
                return ConvertUtils.convertToList(value).stream()
                        .map(j -> writeJsonResult(j))
                        .collect(Collectors.toList());
            case MAP:
                return ((Map<String, Object>) value)
                        .entrySet().stream()
                        .collect(
                                HashMap::new, // workaround for https://bugs.openjdk.java.net/browse/JDK-8148463
                                (mapAccumulator, entry) ->
                                        mapAccumulator.put(entry.getKey(), writeJsonResult(entry.getValue())),
                                HashMap::putAll);
            default:
                return value;
        }
    }

    private static Map<String, Object> relToMap(Relationship rel) {
        Map<String, Object> mapRel = map(
                "id", String.valueOf(rel.getId()),
                "type", RELATIONSHIP,
                "label", rel.getType().toString(),
                "start", nodeToMap(rel.getStartNode()),
                "end", nodeToMap(rel.getEndNode()));

        return mapWithOptionalProps(mapRel, rel.getAllProperties());
    }

    private static Map<String, Object> nodeToMap(Node node) {
        Map<String, Object> mapNode = map("id", String.valueOf(node.getId()));

        mapNode.put("type", NODE);

        if (node.getLabels().iterator().hasNext()) {
            mapNode.put("labels", labelStrings(node));
        }
        return mapWithOptionalProps(mapNode, node.getAllProperties());
    }

    private static Map<String, Object> mapWithOptionalProps(Map<String, Object> mapEntity, Map<String, Object> props) {
        if (!props.isEmpty()) {
            mapEntity.put("properties", props);
        }
        return mapEntity;
    }
}
