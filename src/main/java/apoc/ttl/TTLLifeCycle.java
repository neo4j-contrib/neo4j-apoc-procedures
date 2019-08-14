package apoc.ttl;

import apoc.ApocSettings;
import apoc.util.Util;
import org.neo4j.configuration.Config;
import org.neo4j.graphdb.QueryStatistics;
import org.neo4j.graphdb.Result;
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
    private final Config config;
    private JobHandle ttlIndexJobHandle;
    private JobHandle ttlJobHandle;
    private Log log;

    public TTLLifeCycle(JobScheduler scheduler, GraphDatabaseAPI db, Config config, Log log) {
        this.scheduler = scheduler;
        this.db = db;
        this.config = config;
        this.log = log;
    }

    @Override
    public void start() {

        boolean enabled = config.get(ApocSettings.apoc_ttl_enabled);
        if (enabled) {
            long ttlSchedule = config.get(ApocSettings.apoc_ttl_schedule).getSeconds();
            ttlIndexJobHandle = scheduler.schedule(TTL_GROUP, this::createTTLIndex, (int)(ttlSchedule*0.8), TimeUnit.SECONDS);
            long limit = config.get(ApocSettings.apoc_ttl_limit);
            ttlJobHandle = scheduler.scheduleRecurring(TTL_GROUP, () -> expireNodes(limit), ttlSchedule, ttlSchedule, TimeUnit.SECONDS);
        }
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

    @Override
    public void stop() {
        if (ttlIndexJobHandle != null) ttlIndexJobHandle.cancel(true);
        if (ttlJobHandle != null) ttlJobHandle.cancel(true);
    }
}
