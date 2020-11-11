package apoc.load;

import apoc.ApocConfig;
import apoc.Pools;
import apoc.util.FileUtils;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;

import java.net.URI;
import java.nio.file.*;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;

import static apoc.ApocConfig.*;
import static apoc.periodic.Periodic.JobInfo;

public class LoadDirectoryHandler extends LifecycleAdapter {

    public static final String NOT_ENABLED_ERROR = "Load directory listeners have not been enabled." +
            " Set 'apoc.load.directory.enabled=true' in your apoc.conf file located in the $NEO4J_HOME/conf/ directory.";

    public final ConcurrentHashMap<String, LoadDirectoryItem> directoryListenerList = new ConcurrentHashMap<>();

    private final Log log;
    private final GraphDatabaseService db;
    private final ApocConfig apocConfig;
    private final Pools pools;

    public LoadDirectoryHandler(GraphDatabaseService db,
                                ApocConfig apocConfig,
                                Log log,
                                Pools pools) {
        this.db = db;
        this.apocConfig = apocConfig;
        this.log = log;
        this.pools = pools;
    }

    @Override
    public void start() {
        checkEnabled();
    }

    @Override
    public void stop() {
        removeAll();
    }

    public LoadDirectoryItem remove(String name) {
        checkEnabled();

        LoadDirectoryItem removed = directoryListenerList.remove(name);
        if (removed == null) {
            throw new RuntimeException("Listener with name: " + name + " doesn't exists");
        }

        pools.getJobList().get(new JobInfo(name)).cancel(true);
        pools.getJobList().remove(new JobInfo(name));

        return removed;
    }


    public void checkEnabled() {
        if (!apocConfig.getBoolean(APOC_LOAD_DIRECTORY_ENABLED)) {
            throw new RuntimeException(NOT_ENABLED_ERROR);
        }
    }

    public void add(LoadDirectoryItem loadDirectoryItem) {
        checkEnabled();

        String name = loadDirectoryItem.getName();
        if (directoryListenerList.get(name) != null) {
            pools.getJobList().get(new JobInfo(name)).cancel(true);
        }

        pools.getJobList().put(
                new JobInfo(name),
                pools.getDefaultExecutorService().submit(executeListener(loadDirectoryItem))
        );

        directoryListenerList.put(loadDirectoryItem.getName(), loadDirectoryItem);
    }

    public ConcurrentHashMap<String, LoadDirectoryItem> list() {
        checkEnabled();
        return directoryListenerList;
    }

    public Map<String, LoadDirectoryItem> removeAll() {
        checkEnabled();

        Map<String, LoadDirectoryItem> allToRemove = new HashMap<>(directoryListenerList);
        directoryListenerList.clear();
        pools.getJobList().forEach((key, value) -> value.cancel(true));
        pools.getJobList().clear();

        return allToRemove;
    }

    private Runnable executeListener(LoadDirectoryItem item) {
        return () -> {
            try (WatchService watcher = FileSystems.getDefault().newWatchService()) {

                // todo - create common function with apoc.load.directory()
                String urlDir = item.getUrlDir().isEmpty()
                        ? apocConfig().getString("dbms.directories.import", "import")
                        : FileUtils.changeFileUrlIfImportDirectoryConstrained(item.getUrlDir());

                Path path = Paths.get(URI.create(urlDir).getPath());
                path.register(watcher, item.getConfig().getEventKinds());
                while (!Thread.currentThread().isInterrupted()) {
                    WatchKey watchKey = watcher.take();
                    if (watchKey != null) {
                        watchKey.reset();
                        Path dir = (Path) watchKey.watchable();
                        for (WatchEvent event : watchKey.pollEvents()) {
                            Path eventPath = dir.resolve((Path) event.context());
                            try {
                                WildcardFileFilter fileFilter = new WildcardFileFilter(item.getPattern());
                                boolean matchFilePattern = fileFilter.accept(dir.toFile(), eventPath.getFileName().toString());
                                if (matchFilePattern) {
                                    try (Transaction tx = db.beginTx()) {
                                        tx.execute(item.getCypher());
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
