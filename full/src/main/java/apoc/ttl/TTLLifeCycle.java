/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package apoc.ttl;

import apoc.TTLConfig;
import apoc.util.Util;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.scheduler.JobScheduler;

/**
 * @author mh
 * @since 15.02.17
 */
public class TTLLifeCycle extends LifecycleAdapter {

    private static final Group TTL_GROUP = Group.INDEX_UPDATING;
    private final JobScheduler scheduler;
    private final GraphDatabaseAPI db;
    private JobHandle ttlIndexJobHandle;
    private JobHandle ttlJobHandle;
    private final TTLConfig ttlConfig;
    private final Log log;

    public TTLLifeCycle(
            JobScheduler scheduler, GraphDatabaseAPI db, ApocConfig apocConfig, TTLConfig ttlConfig, Log log) {
        this.scheduler = scheduler;
        this.db = db;
        this.ttlConfig = ttlConfig;
        this.log = log;
    }

    @Override
    public void start() {
        TTLConfig.Values configValues = ttlConfig.configFor(db);
        if (configValues.enabled) {
            long ttlScheduleDb = configValues.schedule;
            ttlIndexJobHandle =
                    scheduler.schedule(TTL_GROUP, this::createTTLIndex, (int) (ttlScheduleDb * 0.8), TimeUnit.SECONDS);
            long limitDb = configValues.limit;
            ttlJobHandle = scheduler.scheduleRecurring(
                    TTL_GROUP, () -> expireNodes(limitDb), ttlScheduleDb, ttlScheduleDb, TimeUnit.SECONDS);
        }
    }

    public void expireNodes(long limit) {
        try {
            if (!Util.isWriteableInstance(db)) return;

            final String matchTTL = "MATCH (t:TTL) WHERE t.ttl < timestamp() ";
            final String queryRels = matchTTL + "WITH t MATCH (t)-[r]-() WITH r LIMIT $limit DELETE r RETURN r";
            final String queryNodes = matchTTL +  "WITH t LIMIT $limit DETACH DELETE t RETURN t";

            long relationshipsDeleted = deleteEntities(limit, queryRels);
            long nodesDeleted = deleteEntities(limit, queryNodes);

            if (nodesDeleted > 0) {
                log.info("TTL: Expired %d nodes %d relationships", nodesDeleted, relationshipsDeleted);
            }
        } catch (Exception e) {
            log.error("TTL: Error deleting expired nodes", e);
        }
    }


    private long deleteEntities(long limit, String query) {
        return deleteEntities(limit, query, 0);
    }

    /**
     * Delete entities in batches, until the result is 0
     */
    private long deleteEntities(long limit, String query, long deleted) {
        long currDeleted = db.executeTransactionally(query, Map.of("limit", limit),
                Iterators::count);

        if (currDeleted > 0) {
            deleted += currDeleted;
            return deleteEntities(limit, query, deleted);
        }
        return deleted;
    }

    public void createTTLIndex() {
        try {
            db.executeTransactionally("CREATE INDEX IF NOT EXISTS FOR (n:TTL) ON (n.ttl)");
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
