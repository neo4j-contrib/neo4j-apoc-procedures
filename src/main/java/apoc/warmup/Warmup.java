package apoc.warmup;

import org.neo4j.procedure.Description;
import apoc.util.Util;
import org.neo4j.cursor.Cursor;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordStorageEngine;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.StoreAccess;
import org.neo4j.kernel.impl.store.format.standard.NodeRecordFormat;
import org.neo4j.kernel.impl.store.format.standard.RelationshipRecordFormat;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Procedure;
import org.neo4j.storageengine.api.NodeItem;
import org.neo4j.storageengine.api.RelationshipItem;

import java.util.stream.Stream;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * @author Sascha Peukert
 * @since 06.05.16
 */
public class Warmup {

    @Context
    public GraphDatabaseAPI db;

    public Warmup(GraphDatabaseAPI db) {
        this.db = db;
    }

    public Warmup() {
    }


    @Procedure
    @Description("apoc.warmup.run() - quickly loads all nodes and rels into memory by skipping one page at a time")
    public Stream<WarmupResult> run(){

        int pageSize = 1 << 13; // default page size, todo read from config
        int nodesPerPage = pageSize / NodeRecordFormat.RECORD_SIZE;
        int relsPerPage = pageSize / RelationshipRecordFormat.RECORD_SIZE;
        long nodesTotal = Util.nodeCount(db);
        long relsTotal = Util.relCount(db);
        long nodesLoaded = 0, relsLoaded = 0;
        long start, timeNodes, timeRels;

        StoreAccess storeAccess = new StoreAccess(db.getDependencyResolver()
                .resolveDependency(RecordStorageEngine.class).testAccessNeoStores());
        NeoStores neoStore = storeAccess.getRawNeoStores();
        long highestNodeKey = neoStore.getNodeStore().getHighestPossibleIdInUse();
        long highestRelationshipKey = neoStore.getRelationshipStore().getHighestPossibleIdInUse();

        try (Transaction tx = db.beginTx()) {
            ThreadToStatementContextBridge ctx = db.getDependencyResolver().resolveDependency(ThreadToStatementContextBridge.class);
            ReadOperations readOperations = ctx.get().readOperations();

            // Load specific nodes and rels
            start = System.nanoTime();
            for (int i = 0; i <= highestNodeKey; i += nodesPerPage) {
                nodesLoaded++;
                loadNode(i, readOperations);
            }
            timeNodes = System.nanoTime();
            for(int i = 0; i <= highestRelationshipKey; i += relsPerPage) {
                relsLoaded++;
                loadRelationship(i, readOperations);
            }
            timeRels = System.nanoTime();
            tx.success();
        }

        WarmupResult result = new WarmupResult(
                pageSize,
                nodesPerPage, nodesTotal, nodesLoaded, NANOSECONDS.toSeconds(timeNodes - start),
                relsPerPage, relsTotal, relsLoaded, NANOSECONDS.toSeconds(timeRels - timeNodes),
                NANOSECONDS.toSeconds(timeRels - start));
        return Stream.of(result);
    }

    private static void loadNode(long id, ReadOperations rOps) {
        try (Cursor<NodeItem> c = rOps.nodeCursor(id)) {
            c.next();
        }
    }

    private static void loadRelationship(long id, ReadOperations rOps) {
        try (Cursor<RelationshipItem> c = rOps.relationshipCursor(id)) {
            c.next();
        }
    }

    public static class WarmupResult {
        public final long pageSize;
        public final long nodesPerPage;
        public final long nodesTotal;
        public final long nodesLoaded;
        public final long nodesTime;

        public final long relsPerPage;
        public final long relsTotal;
        public final long relsLoaded;
        public final long relsTime;
        public final long totalTime;

        public WarmupResult(long pageSize, long nodesPerPage, long nodesTotal, long nodesLoaded, long nodesTime, long relsPerPage, long relsTotal, long relsLoaded, long relsTime, long totalTime) {
            this.pageSize = pageSize;
            this.nodesPerPage = nodesPerPage;
            this.nodesTotal = nodesTotal;
            this.nodesLoaded = nodesLoaded;
            this.nodesTime = nodesTime;
            this.relsPerPage = relsPerPage;
            this.relsTotal = relsTotal;
            this.relsLoaded = relsLoaded;
            this.relsTime = relsTime;
            this.totalTime = totalTime;
        }
    }

}
