package apoc.load;

public class LoadDirectoryItemBase {

    public final String pattern;
    public final String cypher;
    public final String name;
    public final String urlDir;

    public LoadDirectoryItemBase(String name, String pattern, String cypher, String urlDir) {
        this.name = name;
        this.cypher = cypher;
        this.pattern = pattern;
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

}
