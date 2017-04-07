package apoc.export.util;

import java.io.PrintWriter;

/**
 * @author AgileLARUS
 *
 * @since 06-04-2017
 */
public enum ExportFormat {

    NEO4J_SHELL("neo4j-shell", String.format("commit%n"), String.format("begin%n"), String.format("schema await%n")),
    CYPHER_SHELL("cypher-shell", String.format(":commit%n"), String.format(":begin%n"), ""),
    PLAIN_FORMAT("plain", "", "", "");

    private final String format;

    private String commit;

    private String begin;

    private String schemaAwait;

    ExportFormat(String format, String commit, String begin, String schemaAwait) {
        this.format = format;
        this.begin = begin;
        this.commit = commit;
        this.schemaAwait = schemaAwait;
    }

    public static final ExportFormat fromString(String format) {
        if(format != null && !format.isEmpty()){
            for (ExportFormat exportFormat : ExportFormat.values()) {
                if (exportFormat.format.equalsIgnoreCase(format)) {
                    return exportFormat;
                }
            }
        }
        return NEO4J_SHELL;
    }

    public String begin(){
        return this.begin;
    }

    public String commit(){
        return this.commit;
    }

    public String schemaAwait(){
        return this.schemaAwait;
    }
}
