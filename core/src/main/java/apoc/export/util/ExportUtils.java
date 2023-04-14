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

import apoc.export.cypher.ExportFileManager;
import apoc.result.ProgressInfo;
import apoc.util.QueueBasedSpliterator;
import apoc.util.QueueUtil;
import apoc.util.Util;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.procedure.TerminationGuard;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class ExportUtils {
    private ExportUtils() {}

    public static Stream<ProgressInfo> getProgressInfoStream(GraphDatabaseService db,
                                                      ExecutorService executorService,
                                                      TerminationGuard terminationGuard,
                                                      String format,
                                                      ExportConfig exportConfig,
                                                      ProgressReporter reporter,
                                                      ExportFileManager cypherFileManager,
                                                      Consumer<ProgressReporter> dump) {
        long timeout = exportConfig.getTimeoutSeconds();
        final ArrayBlockingQueue<ProgressInfo> queue = new ArrayBlockingQueue<>(1000);
        ProgressReporter reporterWithConsumer = reporter.withConsumer(
                (pi) -> QueueUtil.put(queue, pi == ProgressInfo.EMPTY ? ProgressInfo.EMPTY : new ProgressInfo(pi).drain(cypherFileManager.getStringWriter(format), exportConfig), timeout)
        );
        Util.inTxFuture(null, executorService, db, tx -> {
            dump.accept(reporterWithConsumer);
            return true;
        }, 0, _ignored -> {}, _ignored -> QueueUtil.put(queue, ProgressInfo.EMPTY, timeout));
        QueueBasedSpliterator<ProgressInfo> spliterator = new QueueBasedSpliterator<>(queue, ProgressInfo.EMPTY, terminationGuard, (int) timeout);
        return StreamSupport.stream(spliterator, false);
    }
}
