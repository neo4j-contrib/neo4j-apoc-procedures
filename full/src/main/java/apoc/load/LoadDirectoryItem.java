package apoc.load;

import org.apache.commons.lang3.StringUtils;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

public class LoadDirectoryItem {

    public static final String EVENT_KINDS = "eventKinds";
    public static final String INTERVAL = "interval";
    public static final List<String> DEFAULT_EVENT_KINDS = List.of("ENTRY_CREATE", "ENTRY_DELETE", "ENTRY_MODIFY");

    public static class LoadDirectoryConfig {

        private final List<String> eventKinds;
        private final Long interval;

        public LoadDirectoryConfig( Map<String, Object> config) {
            if (config == null) config = Collections.emptyMap();
            this.interval = (Long) config.getOrDefault("interval", 1000L);
            this.eventKinds = (List<String>) config.getOrDefault("eventKinds", DEFAULT_EVENT_KINDS);
        }

        public List<String> getEventKinds() {
            return eventKinds;
        }

        public Long getInterval() {
            return interval;
        }
    }

    public static class LoadDirectoryResult {

        public final String name;
        public final String status;
        public final String pattern;
        public final String cypher;
        public final String urlDir;
        public final Map<String, Object> config;
        public final String error;

        public LoadDirectoryResult(String name, String status, String pattern, String cypher, String urlDir, LoadDirectoryConfig configClass, String error) {
            this.name = name;
            this.status = status;
            this.pattern = pattern;
            this.cypher = cypher;
            this.urlDir = urlDir;
            this.config = Map.of(EVENT_KINDS, configClass.getEventKinds(), INTERVAL, configClass.getInterval());
            this.error = error;
        }
    }

    enum Status { RUNNING, STOPPED, ERROR }

    private final String pattern;
    private final String cypher;
    private final String name;
    private final String urlDir;
    private final LoadDirectoryConfig config;
    private final AtomicReference<Status> status;
    private AtomicReference<String> error;

    public LoadDirectoryItem(String name) {
        this(name, null, null, null, null);
    }

    public LoadDirectoryItem(String name, String pattern, String cypher, String urlDir, LoadDirectoryConfig config) {
        this.name = name;
        this.cypher = cypher;
        this.pattern = pattern;
        this.urlDir = urlDir;
        this.config = config;
        this.status = new AtomicReference<>(Status.STOPPED);
        this.error = new AtomicReference<>(StringUtils.EMPTY);
    }

    public String getPattern() {
        return pattern;
    }

    public String getCypher() {
        return cypher;
    }

    public String getName() {
        return name;
    }

    public String getUrlDir() {
        return urlDir;
    }

    public LoadDirectoryConfig getConfig() {
        return config;
    }

    public synchronized void setError(String errorMessage) {
        error.set(errorMessage);
        status.set(Status.ERROR);
    }

    public void setStatusRunning() {
        status.set(Status.RUNNING);
    }

    public synchronized LoadDirectoryResult toResult() {
        return new LoadDirectoryResult(name, status.get().name(), pattern, cypher, urlDir, config, error.get());
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
