package apoc.export.util;

import static java.lang.String.format;

/**
 * @author AgileLARUS
 *
 * @since 06-04-2017
 */
public enum ExportFormat {

    NEO4J_SHELL("neo4j-shell",
            format("COMMIT%n"), format("BEGIN%n"), format("SCHEMA AWAIT%n"), ""),

    CYPHER_SHELL("cypher-shell",
            format(":commit%n"), format(":begin%n"), "", "CALL db.awaitIndexes(%d);%n"),

    PLAIN_FORMAT("plain", "", "", "", ""),

    GEPHI("gephi", "", "", "", "");


    private final String format;

    private String commit;

    private String begin;

    private String indexAwait;

    private String schemaAwait;

    ExportFormat(String format, String commit, String begin, String schemaAwait, String indexAwait) {
        this.format = format;
        this.begin = begin;
        this.commit = commit;
        this.schemaAwait = schemaAwait;
        this.indexAwait = indexAwait;
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

    public String indexAwait(long millis){
        return format(this.indexAwait, millis);
    }
}
