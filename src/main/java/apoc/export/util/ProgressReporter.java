package apoc.export.util;

import apoc.result.ProgressInfo;
import org.neo4j.graphdb.QueryStatistics;

import java.io.PrintWriter;
import java.util.stream.Stream;

/**
 * @author mh
 * @since 22.05.16
 */
public class ProgressReporter implements Reporter {
    private final SizeCounter sizeCounter;
    private final PrintWriter out;
    long time;
    int counter;
    long start=System.currentTimeMillis();
    private final ProgressInfo progressInfo;

    public ProgressReporter(SizeCounter sizeCounter, PrintWriter out, ProgressInfo progressInfo) {
        this.sizeCounter = sizeCounter;
        this.out = out;
        this.time = start;
        this.progressInfo = progressInfo;
    }

    @Override
    public void progress(String msg) {
        long now = System.currentTimeMillis();
        // todo report percentages back
        println(String.format(msg+" %d. %d%%: %s time %d ms total %d ms", counter++, percent(), progressInfo, now - time, now - start));
        time = now;
    }

    private void println(String message) {
        if (out!=null) out.println(message);
    }

    private long percent() {
        return sizeCounter == null ? 100 : sizeCounter.getPercent();
    }

    public void update(long nodes, long relationships, long properties) {
        progressInfo.update(nodes,relationships,properties);
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
    }
}
