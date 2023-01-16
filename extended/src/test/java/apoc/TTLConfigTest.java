package apoc;

import static apoc.TTLConfig.DEFAULT_SCHEDULE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.commons.configuration2.Configuration;
import org.junit.Test;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

public class TTLConfigTest
{
    @Test
    public void ttlDisabled() {
        ApocConfig apocConfig = mock(ApocConfig.class);
        Configuration config = mock(Configuration.class);

        when(apocConfig.getConfig()).thenReturn(config);
        when(config.getBoolean(ExtendedApocConfig.APOC_TTL_ENABLED, false)).thenReturn(false);
        when(config.getBoolean("apoc.ttl.enabled.foo", false)).thenReturn(false);

        GraphDatabaseAPI db = mock(GraphDatabaseAPI.class);
        when(db.databaseName()).thenReturn("foo");

        TTLConfig ttlConfig = new TTLConfig(apocConfig, mock(GlobalProcedures.class));

        assertFalse(ttlConfig.configFor(db).enabled);
    }

    @Test
    public void ttlDisabledForOurDatabase() {
        ApocConfig apocConfig = mock(ApocConfig.class);
        Configuration config = mock(Configuration.class);

        when(apocConfig.getConfig()).thenReturn(config);
        when(config.getBoolean(ExtendedApocConfig.APOC_TTL_ENABLED, false)).thenReturn(true);
        when(config.getBoolean("apoc.ttl.enabled.foo", false)).thenReturn(false);

        GraphDatabaseAPI db = mock(GraphDatabaseAPI.class);
        when(db.databaseName()).thenReturn("foo");

        TTLConfig ttlConfig = new TTLConfig(apocConfig, mock(GlobalProcedures.class));

        assertFalse(ttlConfig.configFor(db).enabled);
    }

    @Test
    public void ttlEnabledForOurDatabaseOverridesGlobalSettings() {
        ApocConfig apocConfig = mock(ApocConfig.class);
        Configuration config = mock(Configuration.class);

        when(apocConfig.getConfig()).thenReturn(config);
        when(config.getBoolean(ExtendedApocConfig.APOC_TTL_ENABLED, false)).thenReturn(false);
        when(config.getBoolean("apoc.ttl.enabled.foo", false)).thenReturn(true);

        when(apocConfig.getInt(ExtendedApocConfig.APOC_TTL_SCHEDULE, DEFAULT_SCHEDULE)).thenReturn(300);
        when(apocConfig.getInt("apoc.ttl.schedule.foo", 300)).thenReturn(500);

        when(apocConfig.getInt(ExtendedApocConfig.APOC_TTL_LIMIT, 1000)).thenReturn(5000);
        when(apocConfig.getInt("apoc.ttl.limit.foo", 5000)).thenReturn(1000);

        GraphDatabaseAPI db = mock(GraphDatabaseAPI.class);
        when(db.databaseName()).thenReturn("foo");

        TTLConfig.Values values = new TTLConfig(apocConfig, mock(GlobalProcedures.class)).configFor(db);

        assertTrue(values.enabled);
        assertEquals(500, values.schedule);
        assertEquals(1000, values.limit);
    }

}
