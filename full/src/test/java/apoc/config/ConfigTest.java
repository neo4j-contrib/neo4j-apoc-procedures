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
package apoc.config;

import apoc.util.TestUtil;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.ProvideSystemProperty;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.Map;
import java.util.Set;

import static apoc.ApocConfig.APOC_CONFIG_JOBS_SCHEDULED_NUM_THREADS;
import static apoc.ApocConfig.APOC_EXPORT_FILE_ENABLED;
import static apoc.ApocConfig.APOC_IMPORT_FILE_ALLOW__READ__FROM__FILESYSTEM;
import static apoc.ApocConfig.APOC_IMPORT_FILE_ENABLED;
import static apoc.ApocConfig.APOC_IMPORT_FILE_USE_NEO4J_CONFIG;
import static apoc.ApocConfig.APOC_TRIGGER_ENABLED;
import static apoc.ApocConfig.APOC_TTL_ENABLED;
import static apoc.ApocConfig.APOC_TTL_LIMIT;
import static apoc.ApocConfig.APOC_TTL_SCHEDULE;
import static apoc.ApocConfig.APOC_UUID_ENABLED;
import static apoc.trigger.TriggerHandler.TRIGGER_REFRESH;
import static java.util.Map.entry;
import static org.junit.Assert.assertEquals;

/**
 * @author mh
 * @since 28.10.16
 */
public class ConfigTest {
    // the following configs are always set by default:
    // apoc.export.file.enabled, apoc.import.file.enabled, apoc.import.file.use_neo4j_config, apoc.trigger.enabled and apoc.import.file.allow_read_from_filesystem
    // apoc.ttl.enabled, apoc.ttl.limit, apoc.ttl.schedule
    private static final Map<String, String> EXPECTED_APOC_CONFS = Map.ofEntries(
            entry(APOC_EXPORT_FILE_ENABLED, "false"),
            entry(APOC_IMPORT_FILE_ALLOW__READ__FROM__FILESYSTEM, "true"),
            entry(APOC_IMPORT_FILE_ENABLED, "false"),
            entry(APOC_IMPORT_FILE_USE_NEO4J_CONFIG, "true"),
            entry(APOC_CONFIG_JOBS_SCHEDULED_NUM_THREADS, "4"),
            entry(TRIGGER_REFRESH, "2000"),
            entry(APOC_TRIGGER_ENABLED, "false"),
            entry(APOC_TTL_ENABLED, "false"),
            entry(APOC_UUID_ENABLED, "false"),
            entry(APOC_TTL_LIMIT, "1000"),
            entry(APOC_TTL_SCHEDULE, "PT1M")
    );

    @Rule
    public final ProvideSystemProperty systemPropertyRule
            = new ProvideSystemProperty("foo", "bar")
            .and("apoc.import.enabled", "true")
            .and("apoc.trigger.refresh", "2000")
            .and("apoc.jobs.scheduled.num_threads", "4")
            .and("apoc.static.test", "one")
            .and("apoc.jdbc.cassandra_songs.url", "jdbc:cassandra://localhost:9042/playlist");

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();

    @Before
    public void setUp() throws Exception {
        TestUtil.registerProcedure(db, Config.class);
    }

    @Test
    public void configListTest(){
        TestUtil.testResult(db, "CALL apoc.config.list() YIELD key RETURN * ORDER BY key", r -> {
            final Set<String> actualConfs = Iterators.asSet(r.columnAs("key"));
            assertEquals(EXPECTED_APOC_CONFS.keySet(), actualConfs);
        });
    }
    
    @Test
    public void configMapTest(){
        TestUtil.testCall(db, "CALL apoc.config.map()", r -> {
            assertEquals(EXPECTED_APOC_CONFS, r.get("value"));
        });
    }

}
