package apoc.export.util;

import apoc.result.ProgressInfo;
import apoc.util.JsonUtil;
import org.neo4j.graphdb.QueryStatistics;
import org.neo4j.graphdb.Transaction;

import java.io.PrintWriter;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static apoc.util.Util.setKernelStatusMap;

/**
 * @author mh
 * @since 22.05.16
 */
public class ProgressReporter implements Reporter {
    private final SizeCounter sizeCounter;
    private final PrintWriter out;
    private final long batchSize;
    public final Transaction tx;
    long time;
    int counter;
    long totalEntities = 0;
    long lastBatch = 0;
    long start = System.currentTimeMillis();
    private final ProgressInfo progressInfo;
    private Consumer<ProgressInfo> consumer;

    public ProgressReporter(SizeCounter sizeCounter, PrintWriter out, ProgressInfo progressInfo, Transaction tx) {
        this.sizeCounter = sizeCounter;
        this.out = out;
        this.time = start;
        this.progressInfo = progressInfo;
        this.batchSize = progressInfo.batchSize;
        this.tx = tx;
    }

    public ProgressReporter withConsumer(Consumer<ProgressInfo> consumer) {
        this.consumer = consumer;
        return this;
    }

    @Override
    public void progress(String msg) {
        long now = System.currentTimeMillis();
        // todo report percentages back
        println(String.format(msg + " %d. %d%%: %s time %d ms total %d ms", counter++, percent(), progressInfo, now - time, now - start));
        time = now;
    }

    private void println(String message) {
        if (out != null) out.println(message);
    }

    private long percent() {
        return sizeCounter == null ? 100 : sizeCounter.getPercent();
    }

    public void update(long nodes, long relationships, long properties) {
        update(nodes, relationships, properties, false);
    }
    
    public void update(long nodes, long relationships, long properties, boolean updateCurrent) {
        time = System.currentTimeMillis();
        progressInfo.update(nodes, relationships, properties);
        totalEntities += nodes + relationships;
        acceptBatch();
        updateStatus(updateCurrent);
    }

    public void acceptBatch() {
        if (batchSize != -1 && totalEntities / batchSize > lastBatch) {
            updateRunningBatch(progressInfo);
            if (consumer != null) {
                consumer.accept(progressInfo);
            }
        }
    }

    public void updateRunningBatch(ProgressInfo progressInfo) {
        lastBatch = Math.max(totalEntities / batchSize,lastBatch);
        progressInfo.batches = lastBatch;
        this.progressInfo.rows = totalEntities;
        this.progressInfo.updateTime(start);
    }

    @Override
    public void done() {
        if (totalEntities / batchSize == lastBatch) lastBatch++;
        updateRunningBatch(progressInfo);
        progressInfo.done(start);
        if (consumer != null) {
            consumer.accept(progressInfo);
        }
        if (consumer != null) {
            consumer.accept(ProgressInfo.EMPTY);
        }
    }

    public static void update(QueryStatistics queryStatistics, Reporter reporter) {
        if (queryStatistics.containsUpdates()) {
            reporter.update(
                    queryStatistics.getNodesCreated() - queryStatistics.getNodesDeleted(),
                    queryStatistics.getRelationshipsCreated() - queryStatistics.getRelationshipsDeleted(),
                    queryStatistics.getPropertiesSet());
        }
    }

    public ProgressInfo getTotal() {
        progressInfo.done(start);
        return progressInfo;
    }

    public Stream<ProgressInfo> stream() {
        return Stream.of(getTotal());
    }

    public void nextRow() {
        this.progressInfo.nextRow();
        this.totalEntities++;
        updateStatus(false);
        acceptBatch();
    }

    public void updateStatus(boolean updateCurrent) {
        final Map<String, Object> statusMap = JsonUtil.convertToMap(this.progressInfo);
        if (updateCurrent) {
            setKernelStatusMap(tx, true, statusMap);
        }
        setKernelStatusMap(tx, progressInfo.nodes + progressInfo.relationships + progressInfo.properties, statusMap);
    }
}
