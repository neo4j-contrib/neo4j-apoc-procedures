package apoc.ttl;

import apoc.ApocConfig;
import apoc.TTLConfig;
import apoc.util.Util;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.scheduler.JobScheduler;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author mh
 * @since 15.02.17
 */
public class TTLLifeCycle extends LifecycleAdapter {

    public static final int INITIAL_DELAY = 30;
    public static final int DEFAULT_SCHEDULE = 60;
    private static final Group TTL_GROUP = Group.INDEX_UPDATING;
    private final JobScheduler scheduler;
    private final GraphDatabaseAPI db;
    private final ApocConfig apocConfig;
    private JobHandle ttlIndexJobHandle;
    private JobHandle ttlJobHandle;
    private TTLConfig ttlConfig;
    private Log log;

    public TTLLifeCycle(JobScheduler scheduler, GraphDatabaseAPI db, ApocConfig apocConfig, TTLConfig ttlConfig, Log log) {
        this.scheduler = scheduler;
        this.db = db;
        this.apocConfig = apocConfig;
        this.ttlConfig = ttlConfig;
        this.log = log;
    }

    @Override
    public void start() {
        TTLConfig.Values configValues = ttlConfig.configFor(db);
        if(configValues.enabled) {
            long ttlScheduleDb = configValues.schedule;
            ttlIndexJobHandle = scheduler.schedule(TTL_GROUP, this::createTTLIndex, (int)(ttlScheduleDb*0.8), TimeUnit.SECONDS);
            long limitDb = configValues.limit;
            ttlJobHandle = scheduler.scheduleRecurring(TTL_GROUP, () -> expireNodes(limitDb), ttlScheduleDb, ttlScheduleDb, TimeUnit.SECONDS);
        }
    }

    public void expireNodes(long limit) {
        try {
            if (!Util.isWriteableInstance(db)) return;
            String matchTTL = "MATCH (t:TTL) WHERE t.ttl < timestamp() ";
            String queryRels = matchTTL + "WITH t MATCH (t)-[r]-() RETURN id(r) as id";
            String queryNodes = matchTTL +  "RETURN id(t) as id";
            Map<String,Object> params = Util.map("batchSize", limit, "queryRels", queryRels, "queryNodes", queryNodes);
            long relationshipsDeleted = db.executeTransactionally(
                    "CALL apoc.periodic.iterate($queryRels, 'MATCH ()-[r]->() WHERE id(r) = id DELETE r', {batchSize: $batchSize})",
                    params,
                    result -> Iterators.single(result.columnAs("total"))
            );

            long nodesDeleted = db.executeTransactionally(
                    "CALL apoc.periodic.iterate($queryNodes, 'MATCH (n) WHERE id(n) = id DELETE n', {batchSize: $batchSize})",
                    params,
                    result -> Iterators.single(result.columnAs("total"))
            );

            if (nodesDeleted > 0) {
                log.info("TTL: Expired %d nodes %d relationships", nodesDeleted, relationshipsDeleted);
            }
        } catch (Exception e) {
            log.error("TTL: Error deleting expired nodes", e);
        }
    }

    public void createTTLIndex() {
        try {
            db.executeTransactionally("call apoc.schema.assert({ TTL: ['ttl'] }, null, false)");
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
