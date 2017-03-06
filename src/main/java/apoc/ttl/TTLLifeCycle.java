package apoc.ttl;

import apoc.ApocConfiguration;
import apoc.util.Util;
import org.neo4j.graphdb.QueryStatistics;
import org.neo4j.graphdb.Result;
import org.neo4j.kernel.impl.util.ApocGroup;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;

import java.util.concurrent.TimeUnit;

/**
 * @author mh
 * @since 15.02.17
 */
public class TTLLifeCycle {

    public static final int INITIAL_DELAY = 30;
    public static final int DEFAULT_SCHEDULE = 60;
    private final JobScheduler scheduler;
    private final GraphDatabaseAPI db;
    private JobScheduler.JobHandle ttlIndexJobHandle;
    private JobScheduler.JobHandle ttlJobHandle;
    private Log log;

    public TTLLifeCycle(JobScheduler scheduler, GraphDatabaseAPI db, Log log) {
        this.scheduler = scheduler;
        this.log = log;
        this.db = db;
    }

    public void start() {
        boolean enabled = Util.toBoolean(ApocConfiguration.get("ttl.enabled", null));
        if (!enabled) return;

        long ttlSchedule = Util.toLong(ApocConfiguration.get("ttl.schedule", DEFAULT_SCHEDULE));
        ttlIndexJobHandle = scheduler.schedule(ApocGroup.TTL_GROUP, this::createTTLIndex, (int)(ttlSchedule*0.8), TimeUnit.SECONDS);

        long limit = Util.toLong(ApocConfiguration.get("ttl.limit", 1000L));

        ttlJobHandle = scheduler.scheduleRecurring(ApocGroup.TTL_GROUP, () -> expireNodes(limit), ttlSchedule, ttlSchedule, TimeUnit.SECONDS);
    }

    public void expireNodes(long limit) {
        try {
            if (!Util.isWriteableInstance(db)) return;
            Result result = db.execute("MATCH (t:TTL) where t.ttl < timestamp() WITH t LIMIT {limit} DETACH DELETE t", Util.map("limit", limit));
            QueryStatistics stats = result.getQueryStatistics();
            result.close();
            if (stats.getNodesDeleted()>0) {
                log.info("TTL: Expired %d nodes %d relationships", stats.getNodesDeleted(), stats.getRelationshipsDeleted());
            }
        } catch (Exception e) {
            log.error("TTL: Error deleting expired nodes", e);
        }
    }

    public void createTTLIndex() {
        try {
            db.execute("CREATE INDEX ON :TTL(ttl)").close();
        } catch (Exception e) {
            log.error("TTL: Error creating index", e);
        }
    }

    public void stop() {
        if (ttlIndexJobHandle != null) ttlIndexJobHandle.cancel(true);
        if (ttlJobHandle != null) ttlJobHandle.cancel(true);
    }
}
