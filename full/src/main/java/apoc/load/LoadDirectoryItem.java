package apoc.load;

public class LoadDirectoryItem {

    private final String pattern;
    private final String cypher;
    private final String name;
    private final String urlDir;
    private final LoadDirectoryConfig config;

    public LoadDirectoryItem(String name, String pattern, String cypher, String urlDir, LoadDirectoryConfig config) {
        this.name = name;
        this.cypher = cypher;
        this.pattern = pattern;
        this.config = config;
        this.urlDir = urlDir;
    }

    public String getPattern() {
        return pattern;
    }

    public String getName() {
        return name;
    }

    public String getCypher() {
        return cypher;
    }

    public String getUrlDir() {
        return urlDir;
    }

    public LoadDirectoryConfig getConfig() {
        return config;
    }
}
