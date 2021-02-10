package apoc.load;


import apoc.Pools;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;

import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Stream;

import static apoc.util.FileUtils.checkIfUrlEmptyAndGetFileUrl;
import static apoc.util.FileUtils.getPathFromUrlString;

public class LoadDirectoryHandler extends LifecycleAdapter {

    public final Map<LoadDirectoryItemResult, Future> storage = new ConcurrentHashMap<>();

    private final Log log;
    private final GraphDatabaseService db;
    private final Pools pools;

    public LoadDirectoryHandler(GraphDatabaseService db, Log log, Pools pools) {
        this.db = db;
        this.log = log;
        this.pools = pools;
    }

    @Override
    public void start() {}

    @Override
    public void stop() {
        removeAll();
    }

    public void remove(String name) {

        final LoadDirectoryItemResult loadDirectoryItemResult = new LoadDirectoryItemResult(name);
        remove(loadDirectoryItemResult);
    }

    private void remove(LoadDirectoryItemResult loadDirectoryItemResult) {
        Future removed = storage.remove(loadDirectoryItemResult);
        if (removed == null) {
            String name = loadDirectoryItemResult.getName();
            throw new RuntimeException("Listener with name: " + name + " doesn't exists");
        }
        removed.cancel(true);
    }

    public void add(LoadDirectoryItemWithConfig loadDirectoryItemWithConfig) {

        String name = loadDirectoryItemWithConfig.getName();
        final LoadDirectoryItemResult loadDirectoryItemResult = new LoadDirectoryItemResult(loadDirectoryItemWithConfig);

        if (storage.containsKey(loadDirectoryItemResult)) {
            remove(name);
        }

        storage.put(loadDirectoryItemResult,
                pools.getDefaultExecutorService().submit(executeListener(loadDirectoryItemWithConfig)));
    }

    public Stream<LoadDirectoryItemResult> list() {
        return Collections.unmodifiableMap(storage).keySet().stream();
    }

    public void removeAll() {

        Set<LoadDirectoryItemResult> keys = new HashSet<>(storage.keySet());
        keys.forEach(this::remove);
    }

    private Runnable executeListener(LoadDirectoryItemWithConfig item) {
        return () -> {
            try (WatchService watcher = FileSystems.getDefault().newWatchService()) {

                String urlDir = checkIfUrlEmptyAndGetFileUrl(item.getUrlDir());
                getPathFromUrlString(urlDir).register(watcher, item.getConfig().getEventKinds());

                while (!Thread.currentThread().isInterrupted()) {

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
                    Thread.sleep(item.getConfig().getInterval());
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
    }
}
