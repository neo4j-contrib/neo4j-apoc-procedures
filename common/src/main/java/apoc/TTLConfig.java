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
