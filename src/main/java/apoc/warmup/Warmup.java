package apoc.warmup;

import apoc.Pools;
import apoc.util.Util;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordStorageEngine;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.StoreAccess;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.kernel.impl.store.format.standard.NodeRecordFormat;
import org.neo4j.kernel.impl.store.format.standard.PropertyRecordFormat;
import org.neo4j.kernel.impl.store.format.standard.RelationshipGroupRecordFormat;
import org.neo4j.kernel.impl.store.format.standard.RelationshipRecordFormat;
import org.neo4j.kernel.impl.store.record.*;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Stream;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * @author Sascha Peukert
 * @since 06.05.16
 */
public class Warmup {

    private static final int BATCH_SIZE = 100_000;
    private static final int PAGE_SIZE = 1 << 13;
    @Context
    public GraphDatabaseAPI db;
    @Context
    public TerminationGuard guard;
    @Context
    public Log log;

    static class Records<T extends AbstractBaseRecord> {
        long highId;
        long pageSize = PAGE_SIZE;
        int recordsPerPage;
        RecordStore<T> store;
        T initalRecord;
        long pages;
        long count;
        long time;

        public Records(RecordStore<T> store, T initalRecord) {
            this(store,initalRecord,store.getHighestPossibleIdInUse()+1);
        }
        public Records(RecordStore<T> store, T initalRecord, long count) {
            this.store = store;
            this.initalRecord = initalRecord;
            this.highId = store.getHighestPossibleIdInUse();
            this.recordsPerPage = store.getRecordsPerPage();
            this.count = count;
        }
        public void load(GraphDatabaseAPI db, TerminationGuard guard) {
            long start = System.nanoTime();
            try {
                this.pages = loadRecords(recordsPerPage, highId, store, initalRecord, db, guard);
            } finally {
               time = NANOSECONDS.toSeconds(System.nanoTime() - start);
            }
        }
    }
    @Procedure
    @Description("apoc.warmup.run() - quickly loads all nodes and rels into memory by skipping one page at a time")
    public Stream<WarmupResult> run(@Name(value = "loadProperties", defaultValue = "false") boolean loadProperties, @Name(value = "loadDynamicProperties", defaultValue = "false") boolean loadDynamicProperties) {
        long nodesTotal = Util.nodeCount(db);
        long relsTotal = Util.relCount(db);

        StoreAccess storeAccess = new StoreAccess(db.getDependencyResolver()
                .resolveDependency(RecordStorageEngine.class).testAccessNeoStores());
        NeoStores neoStore = storeAccess.getRawNeoStores();

        Map<StoreType,Records<?>> records = new LinkedHashMap<>();
        records.put(StoreType.NODE,new Records<>(neoStore.getNodeStore(), new NodeRecord(-1),nodesTotal));
        records.put(StoreType.RELATIONSHIP,new Records<>(neoStore.getRelationshipStore(), new RelationshipRecord(-1),relsTotal));
        records.put(StoreType.RELATIONSHIP_GROUP,new Records<>(neoStore.getRelationshipGroupStore(), new RelationshipGroupRecord(-1)));

        if (loadProperties) {
            records.put(StoreType.PROPERTY,new Records<>(neoStore.getPropertyStore(),new PropertyRecord(-1)));
        }
        if (loadDynamicProperties) {
            records.put(StoreType.PROPERTY_STRING,new Records<>(neoStore.getRecordStore(StoreType.PROPERTY_STRING),new DynamicRecord(-1)));
            records.put(StoreType.PROPERTY_ARRAY, new Records<>(neoStore.getRecordStore(StoreType.PROPERTY_ARRAY),new DynamicRecord(-1)));
        }
        records.values().parallelStream().forEach((r)->r.load(db,guard));

        WarmupResult result = new WarmupResult(
                PAGE_SIZE,
                records.get(StoreType.NODE),
                records.get(StoreType.RELATIONSHIP),
                records.get(StoreType.RELATIONSHIP_GROUP),
                loadProperties,
                records.get(StoreType.PROPERTY),
                records.values().stream().mapToLong((r)->r.time).sum(),
                Util.transactionIsTerminated(guard),
                loadDynamicProperties,
                records.get(StoreType.PROPERTY_STRING),
                records.get(StoreType.PROPERTY_ARRAY)
                );
        return Stream.of(result);
    }

    public static <R extends AbstractBaseRecord> long loadRecords(int recordsPerPage, long highestRecordId, RecordStore<R> recordStore, R record, GraphDatabaseAPI db, TerminationGuard guard) {
        long[] ids = new long[BATCH_SIZE];
        long pages = 0;
        int idx = 0;
        List<Future<Long>> futures = new ArrayList<>(100);
        for (long id = 0; id <= highestRecordId; id += recordsPerPage) {
            ids[idx++] = id;
            if (idx == BATCH_SIZE) {
                long[] submitted = ids.clone();
                idx = 0;
                futures.add(Util.inTxFuture(Pools.DEFAULT, db, () -> loadRecords(submitted, record, recordStore, guard)));
            }
            pages += removeDone(futures, false);
        }
        if (idx > 0) {
            long[] submitted = Arrays.copyOf(ids, idx);
            futures.add(Util.inTxFuture(Pools.DEFAULT, db, () -> loadRecords(submitted, record, recordStore, guard)));
        }
        pages += removeDone(futures, true);
        return pages;
    }

    public static <R extends AbstractBaseRecord> long loadRecords(long[] submitted, R record, RecordStore<R> recordStore, TerminationGuard guard) {
        if (Util.transactionIsTerminated(guard)) return 0;
        for (long recordId : submitted) {
            record.setId(recordId);
            record.clear();
            try {
                recordStore.getRecord(recordId, record, RecordLoad.NORMAL);
            } catch (Exception ignore) {
                // ignore
            }
        }
        return submitted.length;
    }

    public static long removeDone(List<Future<Long>> futures, boolean wait) {
        long pages = 0;
        if (wait || futures.size() > 25) {
            Iterator<Future<Long>> it = futures.iterator();
            while (it.hasNext()) {
                Future<Long> future = it.next();
                if (wait || future.isDone()) {
                    try {
                        pages += future.get();
                    } catch (InterruptedException | ExecutionException e) {
                        // log.warn("Error during task execution", e);
                    }
                    it.remove();
                }
            }
        }
        return pages;
    }

    public static class WarmupResult {
        public final long pageSize;
        public final long totalTime;
        public final boolean transactionWasTerminated;

        public final long nodesPerPage;
        public final long nodesTotal;
        public final long nodePages;
        public final long nodesTime;

        public final long relsPerPage;
        public final long relsTotal;
        public final long relPages;
        public final long relsTime;
        public final long relGroupsPerPage;
        public final long relGroupsTotal;
        public final long relGroupPages;
        public final long relGroupsTime;
        public final boolean propertiesLoaded;
        public final boolean dynamicPropertiesLoaded;
        public long propsPerPage;
        public long propRecordsTotal;
        public long propPages;
        public long propsTime;
        public long stringPropsPerPage;
        public long stringPropRecordsTotal;
        public long stringPropPages;
        public long stringPropsTime;
        public long arrayPropsPerPage;
        public long arrayPropRecordsTotal;
        public long arrayPropPages;
        public long arrayPropsTime;

        public WarmupResult(long pageSize,
                            Records nodes,
                            Records rels,
                            Records relGroups,
                            boolean propertiesLoaded,
                            Records props,
                            long totalTime, boolean transactionWasTerminated,
                            boolean dynamicPropertiesLoaded,
                            Records stringProps,
                            Records arrayProps
                            ) {
            this.pageSize = pageSize;
            this.transactionWasTerminated = transactionWasTerminated;
            this.totalTime = totalTime;
            this.propertiesLoaded = propertiesLoaded;
            this.dynamicPropertiesLoaded = dynamicPropertiesLoaded;

            this.nodesPerPage = nodes.recordsPerPage;
            this.nodesTotal = nodes.count;
            this.nodePages = nodes.pages;
            this.nodesTime = nodes.time;

            this.relsPerPage = rels.recordsPerPage;
            this.relsTotal = rels.count;
            this.relPages = rels.pages;
            this.relsTime = rels.time;

            this.relGroupsPerPage = relGroups.recordsPerPage;
            this.relGroupsTotal = relGroups.count;
            this.relGroupPages = relGroups.pages;
            this.relGroupsTime = relGroups.time;

            if (props!=null) {
                this.propsPerPage = props.recordsPerPage;
                this.propRecordsTotal = props.count;
                this.propPages = props.pages;
                this.propsTime = props.time;
            }
            if (stringProps != null) {
                this.stringPropsPerPage = stringProps.recordsPerPage;
                this.stringPropRecordsTotal = stringProps.count;
                this.stringPropPages = stringProps.pages;
                this.stringPropsTime = stringProps.time;
            }
            if (arrayProps != null) {
                this.arrayPropsPerPage = arrayProps.recordsPerPage;
                this.arrayPropRecordsTotal = arrayProps.count;
                this.arrayPropPages = arrayProps.pages;
                this.arrayPropsTime = arrayProps.time;
            }
        }
    }
}
