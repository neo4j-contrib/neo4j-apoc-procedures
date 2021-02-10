package apoc.load;

import java.util.List;
import java.util.Objects;

public class LoadDirectoryItemResult extends LoadDirectoryItemBase {

    public final Long interval;
    public final List<String> eventKinds;

    public LoadDirectoryItemResult(LoadDirectoryItemWithConfig item) {
        super(item.getName(), item.getPattern(), item.getCypher(), item.getUrlDir());
        LoadDirectoryConfig config = item.getConfig();
        this.interval = config.getInterval();
        this.eventKinds = config.getEventKindsAsString();
    }

    public LoadDirectoryItemResult(String name) {
        super(name, null, null, null);
        this.interval = null;
        this.eventKinds = null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LoadDirectoryItemResult that = (LoadDirectoryItemResult) o;
        return name.equals(that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }
}
