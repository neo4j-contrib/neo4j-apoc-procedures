package apoc.export.util;

/**
 * @author AgileLARUS
 *
 * @since 06-04-2017
 */
public enum ExportFormat {

    CYPHER("cypher", String.format(":COMMIT%n"), String.format(":BEGIN%n"), "CALL db.awaitIndex('%s');%n", ""),
    NEO4J_SHELL("neo4j-shell", String.format("commit%n"), String.format("begin%n"), "", String.format("schema await%n")),
    PLAIN_FORMAT("plain", "", "", "", "");

    private final String format;

    private String commit;

    private String begin;

    private String indexAwait;

    private String schemaAwait;

    ExportFormat(String format, String commit, String begin, String indexAwait, String schemaAwait) {
        this.format = format;
        this.begin = begin;
        this.commit = commit;
        this.indexAwait = indexAwait;
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
        return CYPHER;
    }

    public String begin(){
        return this.begin;
    }

    public String commit(){
        return this.commit;
    }

    public String indexAwait(String indexIdentifier){
        return String.format(this.indexAwait, indexIdentifier);
    }

    public String schemaAwait(){
        return this.schemaAwait;
    }
}
