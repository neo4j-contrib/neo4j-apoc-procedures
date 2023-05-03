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

import org.junit.Test;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import static apoc.TTLConfig.DEFAULT_SCHEDULE;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TTLConfigTest {
    @Test
    public void ttlDisabled() {
        ApocConfig apocConfig = mock(ApocConfig.class);
        when(apocConfig.getBoolean(ApocConfig.APOC_TTL_ENABLED)).thenReturn(false);
        when(apocConfig.getBoolean("apoc.ttl.enabled.foo")).thenReturn(false);

        GraphDatabaseAPI db = mock(GraphDatabaseAPI.class);
        when(db.databaseName()).thenReturn("foo");

        TTLConfig ttlConfig = new TTLConfig(apocConfig, mock(GlobalProcedures.class));

        assertFalse(ttlConfig.configFor(db).enabled);
    }

    @Test
    public void ttlDisabledForOurDatabase() {
        ApocConfig apocConfig = mock(ApocConfig.class);
        when(apocConfig.getBoolean(ApocConfig.APOC_TTL_ENABLED)).thenReturn(true);
        when(apocConfig.getBoolean("apoc.ttl.enabled.foo")).thenReturn(false);

        GraphDatabaseAPI db = mock(GraphDatabaseAPI.class);
        when(db.databaseName()).thenReturn("foo");

        TTLConfig ttlConfig = new TTLConfig(apocConfig, mock(GlobalProcedures.class));

        assertFalse(ttlConfig.configFor(db).enabled);
    }

    @Test
    public void ttlEnabledForOurDatabaseOverridesGlobalSettings() {
        ApocConfig apocConfig = mock(ApocConfig.class);
        when(apocConfig.getBoolean(ApocConfig.APOC_TTL_ENABLED)).thenReturn(false);
        when(apocConfig.getBoolean("apoc.ttl.enabled.foo", false)).thenReturn(true);

        when(apocConfig.getInt(ApocConfig.APOC_TTL_SCHEDULE, DEFAULT_SCHEDULE)).thenReturn(300);
        when(apocConfig.getInt("apoc.ttl.schedule.foo", 300)).thenReturn(500);

        when(apocConfig.getInt(ApocConfig.APOC_TTL_LIMIT, 1000)).thenReturn(5000);
        when(apocConfig.getInt("apoc.ttl.limit.foo", 5000)).thenReturn(1000);

        GraphDatabaseAPI db = mock(GraphDatabaseAPI.class);
        when(db.databaseName()).thenReturn("foo");

        TTLConfig.Values values = new TTLConfig(apocConfig, mock(GlobalProcedures.class)).configFor(db);

        assertTrue(values.enabled);
        assertEquals(500, values.schedule);
        assertEquals(1000, values.limit);
    }

}
