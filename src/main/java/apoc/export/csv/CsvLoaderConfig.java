package apoc.export.csv;

import java.util.Map;

/**
 * Config class to store the configuration for loading the CSV file. Names and defaults are based on the import tool's
 * <a href="http://neo4j.com/docs/operations-manual/current/tools/import/command-line-usage/">command line options</a>.
 */
public class CsvLoaderConfig {

    public static final String DELIMITER = "delimiter";
    public static final String ARRAY_DELIMITER = "arrayDelimiter";
    public static final String QUOTATION_CHARACTER = "quotationCharacter";
    public static final String STRING_IDS = "stringIds";
    public static final String SKIP_LINES = "skipLines";

    public static char DELIMITER_DEFAULT = ',';
    public static char ARRAY_DELIMITER_DEFAULT = ';';
    public static char QUOTATION_CHARACTER_DEFAULT = '"';
    public static boolean STRING_IDS_DEFAULT = true;
    public static int SKIP_LINES_DEFAULT = 1;

    private final char delimiter;
    private final char arrayDelimiter;
    private final char quotationCharacter;
    private final boolean stringIds;
    private final int skipLines;

    private CsvLoaderConfig(Builder builder) {
        this.delimiter = builder.delimiter;
        this.arrayDelimiter = builder.arrayDelimiter;
        this.quotationCharacter = builder.quotationCharacter;
        this.stringIds = builder.stringIds;
        this.skipLines = builder.skipLines;
    }

    public char getDelimiter() {
        return delimiter;
    }

    public char getArrayDelimiter() {
        return arrayDelimiter;
    }

    public char getQuotationCharacter() {
        return quotationCharacter;
    }

    public boolean getStringIds() {
        return stringIds;
    }

    public int getSkipLines() {
        return skipLines;
    }

    /**
     * Creates builder to build {@link CsvLoaderConfig}.
     *
     * @return created builder
     */
    public static Builder builder() {
        return new Builder();
    }

    public static CsvLoaderConfig from(Map<String, Object> config) {
        Builder builder = builder();

        if (config.get(DELIMITER) != null) builder.delimiter((char) config.get(DELIMITER));
        if (config.get(ARRAY_DELIMITER) != null) builder.arrayDelimiter((char) config.get(ARRAY_DELIMITER));
        if (config.get(QUOTATION_CHARACTER) != null) builder.quotationCharacter((char) config.get(QUOTATION_CHARACTER));
        if (config.get(STRING_IDS) != null) builder.stringIds((boolean) config.get(STRING_IDS));
        if (config.get(SKIP_LINES) != null) builder.skipLines((int) config.get(SKIP_LINES));

        return builder.build();
    }

    /**
     * Builder to build {@link CsvLoaderConfig}.
     */
    public static final class Builder {
        private char delimiter = DELIMITER_DEFAULT;
        private char arrayDelimiter = ARRAY_DELIMITER_DEFAULT;
        private char quotationCharacter = QUOTATION_CHARACTER_DEFAULT;
        private boolean stringIds = STRING_IDS_DEFAULT;
        private int skipLines = SKIP_LINES_DEFAULT;

        private Builder() {
        }

        public Builder delimiter(char delimiter) {
            this.delimiter = delimiter;
            return this;
        }

        public Builder arrayDelimiter(char arrayDelimiter) {
            this.arrayDelimiter = arrayDelimiter;
            return this;
        }

        public Builder quotationCharacter(char quotationCharacter) {
            this.quotationCharacter = quotationCharacter;
            return this;
        }

        public Builder stringIds(boolean stringIds) {
            this.stringIds = stringIds;
            return this;
        }

        public Builder skipLines(int skipLines) {
            this.skipLines = skipLines;
            return this;
        }

        public CsvLoaderConfig build() {
            return new CsvLoaderConfig(this);
        }
    }

}
