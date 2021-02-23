package apoc.load;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public class LoadDirectoryItem {

    public static final String EVENT_KINDS = "eventKinds";
    public static final String INTERVAL = "interval";
    public static final List<String> DEFAULT_EVENT_KINDS = List.of("ENTRY_CREATE", "ENTRY_DELETE", "ENTRY_MODIFY");

    public final String pattern;
    public final String cypher;
    public final String name;
    public final String urlDir;
    public final Map<String, Object> config;

    public LoadDirectoryItem(String name) {
        this.name = name;
        this.cypher = null;
        this.pattern = null;
        this.urlDir = null;
        this.config = null;
    }

    public LoadDirectoryItem(String name, String pattern, String cypher, String urlDir, Map<String, Object> config) {
        this.name = name;
        this.cypher = cypher;
        this.pattern = pattern;
        this.urlDir = urlDir;
        config.putIfAbsent(EVENT_KINDS, DEFAULT_EVENT_KINDS);
        config.putIfAbsent(INTERVAL, 1000L);
        this.config = config;
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

    public Map<String, Object> getConfig() {
        return config;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LoadDirectoryItem that = (LoadDirectoryItem) o;
        return name.equals(that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }
}
