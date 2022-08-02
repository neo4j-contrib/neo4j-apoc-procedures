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

import apoc.Pools;
import apoc.result.ByteArrayResult;
import apoc.result.ProgressInfo;
import org.neo4j.cypher.export.SubGraph;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;
import org.neo4j.procedure.TerminationGuard;

import java.util.stream.Stream;

public class ExportArrowService {

    private final GraphDatabaseService db;
    private final Pools pools;
    private final TerminationGuard terminationGuard;
    private final Log logger;
    private final Transaction tx;

    public ExportArrowService(GraphDatabaseService db, Pools pools, TerminationGuard terminationGuard, Log logger, Transaction tx) {
        this.db = db;
        this.pools = pools;
        this.terminationGuard = terminationGuard;
        this.logger = logger;
        this.tx = tx;
    }

    public Stream<ByteArrayResult> stream(Object data, ArrowConfig config) {
        if (data instanceof Result) {
            return new ExportResultStreamStrategy(db, pools, terminationGuard, logger).export((Result) data, config, tx);
        } else {
            return new ExportGraphStreamStrategy(db, pools, terminationGuard, logger).export((SubGraph) data, config, tx);
        }
    }

    public Stream<ProgressInfo> file(String fileName, Object data, ArrowConfig config) {
        if (data instanceof Result) {
            return new ExportResultFileStrategy(fileName, db, pools, terminationGuard, logger).export((Result) data, config, tx);
        } else {
            return new ExportGraphFileStrategy(fileName, db, pools, terminationGuard, logger).export((SubGraph) data, config, tx);
        }
    }
}