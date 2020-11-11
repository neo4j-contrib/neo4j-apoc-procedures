package apoc.load;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.nio.file.WatchEvent.Kind;
import static java.nio.file.StandardWatchEventKinds.*;


public class LoadDirectoryConfig {

    private final Integer interval;
    private final List<String> eventKinds;

    public LoadDirectoryConfig( Map<String, Object> config) {
        if (config == null) config = Collections.emptyMap();
        this.interval = (Integer) config.getOrDefault("interval", 1000);
        this.eventKinds = (List<String>) config.getOrDefault("eventKinds", List.of("ENTRY_CREATE", "ENTRY_DELETE", "ENTRY_MODIFY"));
    }

    public Integer getInterval() {
        return interval;
    }

    public Kind[] getEventKinds() {
        Kind[] kinds = eventKinds.stream().map(item -> {
            switch (item) {
                case "ENTRY_CREATE":
                    return ENTRY_CREATE;
                case "ENTRY_MODIFY":
                    return ENTRY_MODIFY;
                case "ENTRY_DELETE":
                    return ENTRY_DELETE;
                default:
                    throw new UnsupportedOperationException("Event Type not supported: " + item);
            }
        }).toArray(Kind[]::new);

        return kinds;
    }

}
