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
package apoc;

import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

public class TTLConfig extends LifecycleAdapter {
    private final ApocConfig apocConfig;
    public static final int DEFAULT_SCHEDULE = 60;

    public TTLConfig(ApocConfig apocConfig, GlobalProcedures globalProceduresRegistry) {
        this.apocConfig = apocConfig;

        globalProceduresRegistry.registerComponent((Class<TTLConfig>) getClass(), ctx -> this, true);
    }

    public Values configFor(GraphDatabaseAPI db) {
        String apocTTLEnabledDb = String.format(ApocConfig.APOC_TTL_ENABLED_DB, db.databaseName());
        String apocTTLScheduleDb = String.format(ApocConfig.APOC_TTL_SCHEDULE_DB, db.databaseName());
        String apocTTLLimitDb = String.format(ApocConfig.APOC_TTL_LIMIT_DB, db.databaseName());
        boolean enabled = apocConfig.getBoolean(ApocConfig.APOC_TTL_ENABLED);
        boolean dbEnabled = apocConfig.getBoolean(apocTTLEnabledDb, enabled);

        if (dbEnabled) {
            long ttlSchedule = apocConfig.getInt(ApocConfig.APOC_TTL_SCHEDULE, DEFAULT_SCHEDULE);
            long ttlScheduleDb = apocConfig.getInt(apocTTLScheduleDb, (int) ttlSchedule);
            long limit = apocConfig.getInt(ApocConfig.APOC_TTL_LIMIT, 1000);
            long limitDb = apocConfig.getInt(apocTTLLimitDb, (int) limit);

            return new Values(true, ttlScheduleDb, limitDb);
        }

        return new Values(false, -1, -1);
    }


    public static class Values {
        public final boolean enabled;
        public final long schedule;
        public final long limit;

        public Values(boolean enabled, long schedule, long limit) {
            this.enabled = enabled;
            this.schedule = schedule;
            this.limit = limit;
        }

        @Override
        public String toString() {
            return "Values{" +
                    "enabled=" + enabled +
                    ", schedule=" + schedule +
                    ", limit=" + limit +
                    '}';
        }
    }
}
