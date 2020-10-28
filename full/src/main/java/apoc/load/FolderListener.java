package apoc.load;

import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import apoc.load.LoadFolder.FolderListenerResult;

import java.nio.file.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;

import static apoc.ApocConfig.apocConfig;
import static java.nio.file.StandardWatchEventKinds.*;

public class FolderListener extends LifecycleAdapter {

    private FolderListenerResult folderListenerResult;
    private Log log;
    private final BiFunction<WatchEvent.Kind, Path, Exception> strategy;
    private static final WatchEvent.Kind<?>[] WATCH_EVENT_KINDS = {ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY};
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final AtomicBoolean closed = new AtomicBoolean();

    public FolderListener(BiFunction<WatchEvent.Kind, Path, Exception> strategy, Log log, FolderListenerResult folderListenerResult) {
        this.strategy = strategy;
        this.log = log;
        this.folderListenerResult = folderListenerResult;
    }

    @Override
    public void start() {
        executor.execute(() -> fsListener());
    }

    @Override
    public void stop() {
        closed.set(true);
        executor.shutdown();
    }

    public FolderListenerResult getFolderListenerResult() {
        return folderListenerResult;
    }

    private void fsListener() {

        try (WatchService watcher = FileSystems.getDefault().newWatchService()) {
            Path path = Paths.get(apocConfig().getString("dbms.directories.import"));
            path.register(watcher, WATCH_EVENT_KINDS);
            while (!closed.get()) {
                WatchKey watchKey = watcher.poll();
                if (watchKey != null) {
                    watchKey.reset();
                    Path dir = (Path) watchKey.watchable();
                    for (WatchEvent<?> event : watchKey.pollEvents()) {
                        WatchEvent.Kind<?> kind = event.kind();
                        Path eventPath = dir.resolve((Path) event.context());
                        try {
                            WildcardFileFilter fileFilter = new WildcardFileFilter(folderListenerResult.pattern);
                            boolean matchFilePattern = fileFilter.accept(dir.toFile(), eventPath.getFileName().toString());
                            if (matchFilePattern) {
                                strategy.apply(kind, eventPath);
                            }
                        } catch (Exception e) {
                            log.error("Exception while executing strategy, full stack trace is:", e);
                        }
                    }
                }
                Thread.sleep(2000);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
