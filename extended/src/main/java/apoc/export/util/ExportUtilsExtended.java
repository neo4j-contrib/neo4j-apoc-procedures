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
package apoc.export.util;

import apoc.export.cypher.ExportFileManagerExtended;
import apoc.result.ExportProgressInfoExtended;
import apoc.util.QueueBasedSpliteratorExtended;
import apoc.util.QueueUtilExtended;
import apoc.util.UtilExtended;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.procedure.TerminationGuard;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.function.BiConsumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class ExportUtilsExtended {
    private ExportUtilsExtended() {}

    public static Stream<ExportProgressInfoExtended> getProgressInfoStream(
            GraphDatabaseService db,
            ExecutorService executorService,
            TerminationGuard terminationGuard,
            String format,
            ExportConfigExtended exportConfig,
            ProgressReporterExtended reporter,
            ExportFileManagerExtended cypherFileManager,
            BiConsumer<Transaction, ProgressReporterExtended> dump) {
        long timeout = exportConfig.getTimeoutSeconds();
        final ArrayBlockingQueue<ExportProgressInfoExtended> queue = new ArrayBlockingQueue<>(1000);
        ProgressReporterExtended reporterWithConsumer = reporter.withConsumer((pi) -> QueueUtilExtended.put(
                queue,
                pi == ExportProgressInfoExtended.EMPTY
                        ? ExportProgressInfoExtended.EMPTY
                        : new ExportProgressInfoExtended((ExportProgressInfoExtended) pi)
                                .drain(cypherFileManager.getStringWriter(format), exportConfig),
                timeout));
        UtilExtended.inTxFuture(
                null,
                executorService,
                db,
                threadBoundTx -> {
                    dump.accept(threadBoundTx, reporterWithConsumer);
                    return true;
                },
                0,
                _ignored -> {},
                _ignored -> QueueUtilExtended.put(queue, ExportProgressInfoExtended.EMPTY, timeout));
        QueueBasedSpliteratorExtended<ExportProgressInfoExtended> spliterator =
                new QueueBasedSpliteratorExtended<>(queue, ExportProgressInfoExtended.EMPTY, terminationGuard, (int) timeout);
        return StreamSupport.stream(spliterator, false);
    }
}
