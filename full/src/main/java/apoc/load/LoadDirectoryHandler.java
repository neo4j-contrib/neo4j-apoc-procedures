package apoc.load;

import apoc.Pools;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;

import static apoc.load.LoadDirectoryItem.EVENT_KINDS;
import static apoc.load.LoadDirectoryItem.INTERVAL;
import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_DELETE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;
import static java.nio.file.WatchEvent.Kind;

import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.stream.Stream;

import static apoc.util.FileUtils.checkIfUrlEmptyAndGetFileUrl;
import static apoc.util.FileUtils.getPathFromUrlString;

public class LoadDirectoryHandler extends LifecycleAdapter {

    public final Map<LoadDirectoryItem, Future> storage = new ConcurrentHashMap<>();

    private final Log log;
    private final GraphDatabaseService db;
    private final Pools pools;

    public LoadDirectoryHandler(GraphDatabaseService db, Log log, Pools pools) {
        this.db = db;
        this.log = log;
        this.pools = pools;
    }

    private static Kind[] fromListStringToKindArray(List<String> eventKinds) {
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

    @Override
    public void start() {}

    @Override
    public void stop() {
        removeAll();
    }

    public void remove(String name) {

        final LoadDirectoryItem loadDirectoryItem = new LoadDirectoryItem(name);
        remove(loadDirectoryItem);
    }

    private void remove(LoadDirectoryItem loadDirectoryItem) {
        Future removed = storage.remove(loadDirectoryItem);
        if (removed == null) {
            String name = loadDirectoryItem.getName();
            throw new RuntimeException("Listener with name: " + name + " doesn't exists");
        }
        removed.cancel(true);
    }

    public void add(LoadDirectoryItem loadDirectoryItem) {

        if (storage.containsKey(loadDirectoryItem)) {
            remove(loadDirectoryItem);
        }

        storage.put(loadDirectoryItem, pools.getDefaultExecutorService().submit(createListener(loadDirectoryItem)));
    }

    public Stream<LoadDirectoryItem.LoadDirectoryResult> list() {
        return Collections.unmodifiableMap(storage).keySet().stream().map(LoadDirectoryItem::toResult);
    }

    public void removeAll() {

        Set<LoadDirectoryItem> keys = new HashSet<>(storage.keySet());
        keys.forEach(this::remove);
    }

    private Runnable createListener(LoadDirectoryItem item) {
        return () -> {
            try (WatchService watcher = FileSystems.getDefault().newWatchService()) {

                final LoadDirectoryItem.LoadDirectoryConfig config = item.getConfig();

                String urlDir = checkIfUrlEmptyAndGetFileUrl(item.getUrlDir());
                getPathFromUrlString(urlDir).register(watcher, fromListStringToKindArray(config.getEventKinds()));

                item.setStatusRunning();
                while (true) {
                    WatchKey watchKey = watcher.take();
                    if (watchKey != null) {
                        watchKey.reset();
                        Path dir = (Path) watchKey.watchable();
                        for (WatchEvent event : watchKey.pollEvents()) {
                            Path filePath = dir.resolve((Path) event.context());
                            try {
                                WildcardFileFilter fileFilter = new WildcardFileFilter(item.getPattern());
                                final String fileName = filePath.getFileName().toString();
                                boolean matchFilePattern = fileFilter.accept(dir.toFile(), fileName);
                                if (matchFilePattern) {
                                    try (Transaction tx = db.beginTx()) {
                                        tx.execute(item.getCypher(),
                                                Map.of("fileName", fileName,
                                                        "filePath", filePath.toString(),
                                                        "fileDirectory", dir.toString(),
                                                        "eventKind", event.kind().name())
                                        );
                                        tx.commit();
                                    }
                                }
                            } catch (Exception e) {
                                log.error("Exception while executing strategy, full stack trace is:", e);
                            }
                        }
                    }
                    Thread.sleep(config.getInterval());
                }
            } catch (Exception e) {
                item.setError(ExceptionUtils.getStackTrace(e));
            }
        };
    }
}
