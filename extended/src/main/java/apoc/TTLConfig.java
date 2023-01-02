package apoc;

import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

public class TTLConfig extends LifecycleAdapter {
    private final ApocConfig apocConfig;
    public static final int DEFAULT_SCHEDULE = 60;
    private static TTLConfig theInstance;

    public TTLConfig(ApocConfig apocConfig, GlobalProcedures globalProceduresRegistry) {
        this.apocConfig = apocConfig;
        theInstance = this;
        globalProceduresRegistry.registerComponent((Class<TTLConfig>) getClass(), ctx -> this, true);
    }

    public static TTLConfig ttlConfig() {
        return theInstance;
    }

    public Values configFor(GraphDatabaseAPI db) {
        String apocTTLEnabledDb = String.format(ExtendedApocConfig.APOC_TTL_ENABLED_DB, db.databaseName());
        String apocTTLScheduleDb = String.format(ExtendedApocConfig.APOC_TTL_SCHEDULE_DB, db.databaseName());
        String apocTTLLimitDb = String.format(ExtendedApocConfig.APOC_TTL_LIMIT_DB, db.databaseName());
        boolean enabled = apocConfig.getConfig().getBoolean(ExtendedApocConfig.APOC_TTL_ENABLED, false);
        boolean dbEnabled = apocConfig.getConfig().getBoolean(apocTTLEnabledDb, enabled);

        if (dbEnabled) {
            long ttlSchedule = apocConfig.getInt(ExtendedApocConfig.APOC_TTL_SCHEDULE, DEFAULT_SCHEDULE);
            long ttlScheduleDb = apocConfig.getInt(apocTTLScheduleDb, (int) ttlSchedule);
            long limit = apocConfig.getInt(ExtendedApocConfig.APOC_TTL_LIMIT, 1000);
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
