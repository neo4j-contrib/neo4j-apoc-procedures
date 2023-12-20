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
package apoc.export.csv;

import apoc.Pools;
import apoc.export.util.ProgressReporter;
import apoc.result.ProgressInfo;
import apoc.util.Util;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

public class ImportCsv {
    @Context
    public GraphDatabaseService db;

    @Context
    public Pools pools;

    @Context
    public Log log;

    @Context
    public TerminationGuard terminationGuard;

    public ImportCsv(GraphDatabaseService db) {
        this.db = db;
    }

    public ImportCsv() {}

    @Procedure(name = "apoc.import.csv", mode = Mode.SCHEMA)
    @Description(
            "apoc.import.csv(nodes, relationships, config) - imports nodes and relationships from the provided CSV files with given labels and types")
    public Stream<ProgressInfo> importCsv(
            @Name("nodes") List<Map<String, Object>> nodes,
            @Name("relationships") List<Map<String, Object>> relationships,
            @Name("config") Map<String, Object> config)
            throws Exception {
        ProgressInfo result = Util.inThread(pools, () -> {
            String file = "progress.csv";
            String source = "file";
            if (nodes.stream().anyMatch(node -> node.containsKey("data"))) {
                file = null;
                source = "file/binary";
            }
            final CsvLoaderConfig clc = CsvLoaderConfig.from(config);
            final ProgressReporter reporter = new ProgressReporter(null, null, new ProgressInfo(file, source, "csv"));
            final CsvEntityLoader loader = new CsvEntityLoader(clc, reporter, log, terminationGuard);

            final Map<String, Map<String, Long>> idMapping = new HashMap<>();
            for (Map<String, Object> node : nodes) {
                final Object data = node.getOrDefault("fileName", node.get("data"));
                final List<String> labels = (List<String>) node.get("labels");
                loader.loadNodes(data, labels, db, idMapping);
            }

            for (Map<String, Object> relationship : relationships) {
                final Object fileName = relationship.getOrDefault("fileName", relationship.get("data"));
                final String type = (String) relationship.get("type");
                loader.loadRelationships(fileName, type, db, idMapping);
            }

            return reporter.getTotal();
        });
        return Stream.of(result);
    }
}
