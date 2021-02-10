package apoc.load;

public class LoadDirectoryItemWithConfig extends LoadDirectoryItemBase {

    private final LoadDirectoryConfig config;

    public LoadDirectoryItemWithConfig(String name, String pattern, String cypher, String urlDir, LoadDirectoryConfig config) {
        super(name, pattern, cypher, urlDir);
        this.config = config;
    }

    public LoadDirectoryConfig getConfig() {
        return config;
    }
}
