package apoc.warmup;

import apoc.util.Util;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordStorageEngine;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.StoreAccess;
import org.neo4j.kernel.impl.store.format.standard.NodeRecordFormat;
import org.neo4j.kernel.impl.store.format.standard.PropertyRecordFormat;
import org.neo4j.kernel.impl.store.format.standard.RelationshipRecordFormat;
import org.neo4j.kernel.impl.store.record.*;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.stream.Stream;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * @author Sascha Peukert
 * @since 06.05.16
 */
public class Warmup {

    @Context
    public GraphDatabaseAPI db;

    @Procedure
    @Description("apoc.warmup.run() - quickly loads all nodes and rels into memory by skipping one page at a time")
    public Stream<WarmupResult> run(@Name(value = "loadProperties",defaultValue = "false") boolean loadProperties) {

        int pageSize = 1 << 13; // default page size, todo read from config
        int nodesPerPage = pageSize / NodeRecordFormat.RECORD_SIZE;
        int relsPerPage = pageSize / RelationshipRecordFormat.RECORD_SIZE;
        int propsPerPage = pageSize / PropertyRecordFormat.RECORD_SIZE;
        long nodesTotal = Util.nodeCount(db);
        long relsTotal = Util.relCount(db);
        long nodePages = 0, relPages = 0, propPages = 0;
        long start, timeNodes, timeRels, timeProps;

        StoreAccess storeAccess = new StoreAccess(db.getDependencyResolver()
                .resolveDependency(RecordStorageEngine.class).testAccessNeoStores());
        NeoStores neoStore = storeAccess.getRawNeoStores();
        long highestNodeId = neoStore.getNodeStore().getHighestPossibleIdInUse();
        long highestRelationshipId = neoStore.getRelationshipStore().getHighestPossibleIdInUse();
        long highestPropertyId = neoStore.getPropertyStore().getHighestPossibleIdInUse();
        long propRecordsTotal = highestPropertyId+1;

        try (Transaction tx = db.beginTx()) {
            start = System.nanoTime();
            nodePages = loadRecords(nodesPerPage, highestNodeId, neoStore.getNodeStore(), new NodeRecord(-1));
            timeNodes = System.nanoTime();

            relPages = loadRecords(relsPerPage, highestRelationshipId, neoStore.getRelationshipStore(), new RelationshipRecord(-1));
            timeRels = System.nanoTime();

            timeProps = timeRels;
            if (loadProperties) {
                propPages = loadRecords(propsPerPage, highestPropertyId, neoStore.getPropertyStore(), new PropertyRecord(-1));
                timeProps = System.nanoTime();
            }
            tx.success();
        }

        WarmupResult result = new WarmupResult(
                pageSize,
                nodesPerPage, nodesTotal, nodePages, NANOSECONDS.toSeconds(timeNodes - start),
                relsPerPage, relsTotal, relPages, NANOSECONDS.toSeconds(timeRels - timeNodes),
                loadProperties, propsPerPage, propRecordsTotal, propPages, NANOSECONDS.toSeconds(timeProps - timeRels),
                NANOSECONDS.toSeconds(timeProps - start),
                Util.transactionIsTerminated(db));
        return Stream.of(result);
    }

    public <R extends AbstractBaseRecord> long loadRecords(int recordsPerPage, long highestRecordId, RecordStore<R> recordStore, R record) {
        long pages = 0;
        for (long id = 0; id <= highestRecordId; id += recordsPerPage) {
            if (pages % 100_000 == 0) {
                if (Util.transactionIsTerminated(db)) return pages;
            }
            pages++;
            record.setId(id);
            record.clear();
            try {
                recordStore.getRecord(id, record, RecordLoad.NORMAL);
            } catch(Exception ignore) {
                // ignore
            }
        }
        return pages;
    }

    public static class WarmupResult {
        public final long pageSize;
        public final long nodesPerPage;
        public final long nodesTotal;
        public final long nodePages;
        public final long nodesTime;

        public final long relsPerPage;
        public final long relsTotal;
        public final long relPages;
        public final long relsTime;
        public final boolean propertiesLoaded;
        public final long propsPerPage;
        public final long propRecordsTotal;
        public final long propPages;
        public final long propsTime;
        public final long totalTime;
        public final boolean transactionWasTerminated;

        public WarmupResult(long pageSize,
                            long nodesPerPage, long nodesTotal, long nodePages, long nodesTime,
                            long relsPerPage, long relsTotal, long relPages, long relsTime,
                            boolean propertiesLoaded,
                            long propsPerPage, long propRecordsTotal, long propPages, long propsTime,
                            long totalTime, boolean transactionWasTerminated) {
            this.pageSize = pageSize;
            this.nodesPerPage = nodesPerPage;
            this.nodesTotal = nodesTotal;
            this.nodePages = nodePages;
            this.nodesTime = nodesTime;
            this.relsPerPage = relsPerPage;
            this.relsTotal = relsTotal;
            this.relPages = relPages;
            this.relsTime = relsTime;
            this.propertiesLoaded = propertiesLoaded;
            this.propsPerPage = propsPerPage;
            this.propRecordsTotal = propRecordsTotal;
            this.propPages = propPages;
            this.propsTime = propsTime;
            this.totalTime = totalTime;
            this.transactionWasTerminated = transactionWasTerminated;
        }
    }

}
