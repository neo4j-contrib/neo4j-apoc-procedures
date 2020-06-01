package apoc.result;

import java.io.StringWriter;

/**
 * @author mh
 * @since 22.05.16
 */
public class ProgressInfo {
    public static final ProgressInfo EMPTY = new ProgressInfo(null, null, null);
    public final String file;
    public String source;
    public final String format;
    public long nodes;
    public long relationships;
    public long properties;
    public long time;
    public long rows;
    public long batchSize = -1;
    public long batches;
    public boolean done;
    public String data;

    public ProgressInfo(String file, String source, String format) {
        this.file = file;
        this.source = source;
        this.format = format;
    }

    public ProgressInfo(ProgressInfo pi) {
        this.file = pi.file;
        this.source = pi.source;
        this.format = pi.format;
        this.nodes = pi.nodes;
        this.relationships = pi.relationships;
        this.properties = pi.properties;
        this.time = pi.time;
        this.rows = pi.rows;
        this.batchSize = pi.batchSize;
        this.batches = pi.batches;
        this.done = pi.done;
    }

    @Override
    public String toString() {
        return String.format("nodes = %d rels = %d properties = %d", nodes, relationships, properties);
    }

    public ProgressInfo update(long nodes, long relationships, long properties) {
        this.nodes += nodes;
        this.relationships += relationships;
        this.properties += properties;
        return this;
    }

    public ProgressInfo updateTime(long start) {
        this.time = System.currentTimeMillis() - start;
        return this;
    }
    public ProgressInfo done(long start) {
        this.done = true;
        return updateTime(start);
    }

    public void nextRow() {
        this.rows++;
    }

    public ProgressInfo drain(StringWriter writer) {
        if (writer != null) {
            this.data = writer.toString();
            writer.getBuffer().setLength(0);
        }
        return this;
    }
}
