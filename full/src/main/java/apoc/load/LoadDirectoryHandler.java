package apoc.load;

import apoc.ApocConfig;
import apoc.Pools;
import apoc.periodic.Periodic;
import apoc.util.FileUtils;
import apoc.util.Util;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventListener;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;

import java.net.URI;
import java.nio.file.*;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static apoc.ApocConfig.*;
import static java.nio.file.StandardWatchEventKinds.*;

public class LoadDirectoryHandler extends LifecycleAdapter implements TransactionEventListener<Void> {

    // todo - cambiare testo
    public static final String NOT_ENABLED_ERROR = "Load directory listeners have not been enabled." +
            " Set 'apoc.load.directory.enabled=true' in your apoc.conf file located in the $NEO4J_HOME/conf/ directory.";


    // todo - serve?
    private final AtomicBoolean registeredWithKernel = new AtomicBoolean(false);

    // MAPPA DI FOLDER LISTENER
    // todo - classe
    public final ConcurrentHashMap<String, LoadDirectoryItem> directoryListenerList = new ConcurrentHashMap<>();

    // todo - serve?
    private final DatabaseManagementService databaseManagementService;

    private Log log;
    private final GraphDatabaseService db;
    private final ApocConfig apocConfig;

    private final Pools pools;

    private static final WatchEvent.Kind<?>[] WATCH_EVENT_KINDS = {ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY};
    //
    // private final ExecutorService executor = Executors.newSingleThreadExecutor();

    // todo
    private static final AtomicBoolean closed = new AtomicBoolean(false);

    public LoadDirectoryHandler(GraphDatabaseService db, DatabaseManagementService databaseManagementService,
                                ApocConfig apocConfig, Log log, GlobalProcedures globalProceduresRegistry,
                                Pools pools) {
        this.db = db;
        this.databaseManagementService = databaseManagementService;
        this.apocConfig = apocConfig;
        this.log = log;
        this.pools = pools;
    }

    @Override
    public void start() {
        checkEnabled();
        databaseManagementService.registerTransactionEventListener(db.databaseName(), this);

    }

    @Override
    public void stop() {
        checkEnabled();
        databaseManagementService.unregisterTransactionEventListener(db.databaseName(), this);
    }

    public LoadDirectoryItem remove(String name) {
        checkEnabled();

        LoadDirectoryItem previous = directoryListenerList.remove(name);
        closed.set(true);

        refreshListeners();
        return previous;
    }


    public void checkEnabled() {
        if (!apocConfig.getBoolean(APOC_LOAD_DIRECTORY_ENABLED)) {
            throw new RuntimeException(NOT_ENABLED_ERROR);
        }
    }

    @Override
    public Void beforeCommit(TransactionData data, Transaction transaction, GraphDatabaseService databaseService) throws Exception {
        return null;
    }

    @Override
    public void afterCommit(TransactionData data, Void state, GraphDatabaseService databaseService) {
        refreshListeners();
        System.out.println("after commit");
    }

    @Override
    public void afterRollback(TransactionData data, Void state, GraphDatabaseService databaseService) {

    }

    public void add(LoadDirectoryItem loadDirectoryItem) {
        checkEnabled();

        /*
        directoryListenerList.remove(loadDirectoryItem.getName());

         */
        closed.set(true);



        directoryListenerList.put(loadDirectoryItem.getName(), loadDirectoryItem);
        refreshListeners();
    }

    public void refreshListeners() {
        pools.getJobList().entrySet().stream().forEach(item -> {
            ((Future) item.getValue()).cancel(true);
        });
        pools.getJobList().clear();

        directoryListenerList.forEach((name, data) -> {
            ExecutorService executorService = pools.getDefaultExecutorService();
            // todo
            pools.getJobList().put(new Periodic.JobInfo(name), executorService.submit(wrapTask(data)));

            /*
            System.out.println("ciclo");
            Util.inTxFuture(pools.getDefaultExecutorService(), db, (tx) -> {
                executeListeners(tx, data);
                return null;
            });

             */

        });
        System.out.println(pools.getJobList().size());
    }

    public ConcurrentHashMap<String, LoadDirectoryItem> list() {
        checkEnabled();
        return directoryListenerList;
    }

    // todo
    public List<ConcurrentHashMap<String, LoadDirectoryItem>> removeAll() {

        directoryListenerList.clear();
        closed.set(true);
        return null;
    }

    private Runnable wrapTask(LoadDirectoryItem data) {
        return () -> {
            executeListeners(data);
        };
    }

    public void executeListeners(LoadDirectoryItem data) {
        closed.set(false);
        //directoryListenerList.forEach((name, data) -> {

            try (WatchService watcher = FileSystems.getDefault().newWatchService()) {

                // todo -- creare funzione comune con apoc.load.directory()
                String urlDir = data.getUrlDir().isEmpty()
                        ? apocConfig().getString("dbms.directories.import", "import")
                        : FileUtils.changeFileUrlIfImportDirectoryConstrained(data.getUrlDir());
                // --

                System.out.println("urlDir");
                System.out.println(urlDir);
                System.out.println(data.getName());
                Path path = Paths.get(URI.create(urlDir).getPath());
                path.register(watcher, data.getConfig().getEventKinds());
                while (!closed.get()) {
                    WatchKey watchKey = watcher.take();
                    if (watchKey != null) {
                        watchKey.reset();
                        Path dir = (Path) watchKey.watchable();
                        for (WatchEvent event : watchKey.pollEvents()) {
                            System.out.println("faccio cose - " + data.getCypher() + Instant.now());
                            Path eventPath = dir.resolve((Path) event.context());
                            System.out.println(watchKey);
                            System.out.println(eventPath);
                            try {
                                WildcardFileFilter fileFilter = new WildcardFileFilter(data.getPattern());
                                boolean matchFilePattern = fileFilter.accept(dir.toFile(), eventPath.getFileName().toString());
                                if (matchFilePattern) {
                                    /*
                                    try {
                                        //params.put("trigger", name);
                                        Result result = tx.execute(data.getCypher());
                                        tx.commit();
                                        Iterators.count(result);
                                        result.close();
//                    result.close();
                                    } catch (Exception e) {
                                        System.out.println(e.getMessage());
                                       //log.warn("Error executing trigger " + name + " in phase " + phase, e);
                                        //exceptions.put(name, e.getMessage());
                                    }*/

                                    try (Transaction tx = db.beginTx()) {
                                        tx.execute(data.getCypher());
                                        tx.commit();
                                        System.out.println("committo cose");
                                    }


                                }
                            } catch (Exception e) {
                                System.out.println("spacco cose");
                                log.error("Exception while executing strategy, full stack trace is:", e);
                            }
                        }
                    }
                    Thread.sleep(data.getConfig().getInterval());
                }
                System.out.println("finisco cose - "+ data.getCypher() + Instant.now());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

        //});
    }

}
