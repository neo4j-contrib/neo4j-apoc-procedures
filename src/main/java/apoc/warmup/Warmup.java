package apoc.warmup;

import apoc.Description;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.kernel.impl.store.format.standard.NodeRecordFormat;
import org.neo4j.kernel.impl.store.format.standard.RelationshipRecordFormat;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Procedure;

import java.util.Collections;
import java.util.Map;
import java.util.stream.Stream;

import static apoc.util.MapUtil.map;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * @author Sascha Peukert
 * @since 06.05.16
 */
public class Warmup {

    public static final String NODE_COUNT = "MATCH (n) RETURN count(*) as result";
    public static final String REL_COUNT = "MATCH ()-->() RETURN count(*) as result";
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
        long nodesTotal = runNumericQuery(NODE_COUNT,null);
        long relsTotal = runNumericQuery(REL_COUNT,null);

        long start = System.nanoTime();
        long nodesLoaded = runNumericQuery("UNWIND range(0,{maxId}-1,{step}) as id MATCH (n) WHERE id(n) = id return count(*) as result",
                map("maxId",nodesTotal, "step", nodesPerPage));
        long timeNodes = System.nanoTime();
        long relsLoaded = runNumericQuery("UNWIND range(0,{maxId}-1,{step}) as id MATCH ()-[r]->() WHERE id(r) = id return count(*) as result",
                map("maxId",relsTotal, "step", relsPerPage));
        long timeRels = System.nanoTime();

        WarmupResult result = new WarmupResult(
                pageSize,
                nodesPerPage, nodesTotal, nodesLoaded, NANOSECONDS.toSeconds(timeNodes - start),
                relsPerPage, relsTotal, relsLoaded, NANOSECONDS.toSeconds(timeRels - timeNodes),
                NANOSECONDS.toSeconds(timeRels - start));
        return Stream.of(result);
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

    public Long runNumericQuery(String query,Map<String,Object> params) {
        if (params == null) params = Collections.emptyMap();
        try (ResourceIterator<Long> it = db.execute(query,params).<Long>columnAs("result")) {
            return it.next();
        }
    }
}
