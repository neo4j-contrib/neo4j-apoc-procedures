package apoc.ttl;

import apoc.ApocConfig;
import apoc.util.Util;
import org.neo4j.graphdb.QueryStatistics;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.scheduler.JobScheduler;

import java.util.concurrent.TimeUnit;

/**
 * @author mh
 * @since 15.02.17
 */
public class TTLLifeCycle extends LifecycleAdapter {

    public static final int INITIAL_DELAY = 30;
    public static final int DEFAULT_SCHEDULE = 60;
    public static Group TTL_GROUP = Group.INDEX_UPDATING;
    private final JobScheduler scheduler;
    private final GraphDatabaseAPI db;
    private final ApocConfig apocConfig;
    private JobHandle ttlIndexJobHandle;
    private JobHandle ttlJobHandle;
    private Log log;

    public TTLLifeCycle(JobScheduler scheduler, GraphDatabaseAPI db, ApocConfig apocConfig, Log log) {
        this.scheduler = scheduler;
        this.db = db;
        this.apocConfig = apocConfig;
        this.log = log;
    }

    @Override
    public void start() {
        boolean enabled = apocConfig.getBoolean(ApocConfig.APOC_TTL_ENABLED);
        if (enabled) {
            long ttlSchedule = apocConfig.getInt(ApocConfig.APOC_TTL_SCHEDULE, DEFAULT_SCHEDULE);
            ttlIndexJobHandle = scheduler.schedule(TTL_GROUP, this::createTTLIndex, (int)(ttlSchedule*0.8), TimeUnit.SECONDS);
            long limit = apocConfig.getInt(ApocConfig.APOC_TTL_LIMIT, 1000);
            ttlJobHandle = scheduler.scheduleRecurring(TTL_GROUP, () -> expireNodes(limit), ttlSchedule, ttlSchedule, TimeUnit.SECONDS);
        }
    }

    public void expireNodes(long limit) {
        try {
            if (!Util.isWriteableInstance(db)) return;
            db.executeTransactionally("MATCH (t:TTL) where t.ttl < timestamp() WITH t LIMIT $limit DETACH DELETE t",
                    Util.map("limit", limit),
                    result -> {
                        QueryStatistics stats = result.getQueryStatistics();
                        if (stats.getNodesDeleted()>0) {
                            log.info("TTL: Expired %d nodes %d relationships", stats.getNodesDeleted(), stats.getRelationshipsDeleted());
                        }
                        return null;
                    });
        } catch (Exception e) {
            log.error("TTL: Error deleting expired nodes", e);
        }
    }

    public void createTTLIndex() {
        try {
            db.executeTransactionally("CREATE INDEX ON :TTL(ttl)");
        } catch (Exception e) {
            log.error("TTL: Error creating index", e);
        }
    }

    @Override
    public void stop() {
        if (ttlIndexJobHandle != null) ttlIndexJobHandle.cancel();
        if (ttlJobHandle != null) ttlJobHandle.cancel();
    }
}
