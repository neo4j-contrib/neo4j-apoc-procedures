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

import apoc.PoolsExtended;
import apoc.cypher.export.SubGraphExtended;
import apoc.export.util.ProgressReporterExtended;
import apoc.result.ExportProgressInfoExtended;
import apoc.util.collection.IterablesExtended;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.Schema;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.logging.Log;
import org.neo4j.procedure.TerminationGuard;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.stream.Stream;

public class ExportGraphFileStrategy implements ExportArrowFileStrategy<SubGraphExtended>, ExportGraphStrategy {

    private final String fileName;
    private final GraphDatabaseService db;
    private final PoolsExtended pools;
    private final TerminationGuard terminationGuard;
    private final Log logger;

    private final RootAllocator bufferAllocator;

    private Schema schema;

    public ExportGraphFileStrategy(
            String fileName, GraphDatabaseService db, PoolsExtended pools, TerminationGuard terminationGuard, Log logger) {
        this.fileName = fileName;
        this.db = db;
        this.pools = pools;
        this.terminationGuard = terminationGuard;
        this.logger = logger;
        this.bufferAllocator = new RootAllocator();
    }

    @Override
    public Iterator<Map<String, Object>> toIterator(ProgressReporterExtended reporter, SubGraphExtended subGraph) {
        return Stream.concat(IterablesExtended.stream(subGraph.getNodes()), IterablesExtended.stream(subGraph.getRelationships()))
                .map(entity -> {
                    reporter.update(entity instanceof Node ? 1 : 0, entity instanceof Relationship ? 1 : 0, 0);
                    return this.entityToMap(entity);
                })
                .iterator();
    }

    @Override
    public String getSource(SubGraphExtended subGraph) {
        return String.format(
                "graph: nodes(%d), rels(%d)",
                IterablesExtended.count(subGraph.getNodes()), IterablesExtended.count(subGraph.getRelationships()));
    }

    @Override
    public Stream<ExportProgressInfoExtended> export(SubGraphExtended data, ArrowConfig config) {
        schemaFor(List.of(createConfigMap(data, config)));
        return ExportArrowFileStrategy.super.export(data, config);
    }

    @Override
    public String getFileName() {
        return fileName;
    }

    @Override
    public TerminationGuard getTerminationGuard() {
        return terminationGuard;
    }

    @Override
    public BufferAllocator getBufferAllocator() {
        return bufferAllocator;
    }

    @Override
    public GraphDatabaseService getGraphDatabaseApi() {
        return db;
    }

    @Override
    public ExecutorService getExecutorService() {
        return pools.getDefaultExecutorService();
    }

    @Override
    public Log getLogger() {
        return logger;
    }

    @Override
    public synchronized Schema schemaFor(List<Map<String, Object>> records) {
        if (schema == null) {
            schema = schemaFor(getGraphDatabaseApi(), records);
        }
        return schema;
    }
}
