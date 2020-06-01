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
                (pi) -> QueueUtil.put(queue, pi == ProgressInfo.EMPTY ? ProgressInfo.EMPTY : new ProgressInfo(pi).drain(cypherFileManager.getStringWriter(format)), timeout)
        );
        Util.inTxFuture(executorService, db, tx -> {
            dump.accept(reporterWithConsumer);
            return true;
        });
        QueueBasedSpliterator<ProgressInfo> spliterator = new QueueBasedSpliterator<>(queue, ProgressInfo.EMPTY, terminationGuard, (int) timeout);
        return StreamSupport.stream(spliterator, false);
    }
}
