package apoc.config;

import apoc.util.TestUtil;
import apoc.util.collection.Iterators;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.ProvideSystemProperty;
import org.junit.jupiter.api.AfterAll;

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
import static apoc.trigger.TriggerHandler.TRIGGER_REFRESH;
import static org.junit.Assert.assertEquals;

/**
 * @author mh
 * @since 28.10.16
 */
public class ConfigTest {
    // apoc.export.file.enabled, apoc.import.file.enabled, apoc.import.file.use_neo4j_config, apoc.trigger.enabled and apoc.import.file.allow_read_from_filesystem
    // are always set by default
    private static final Map<String, String> EXPECTED_APOC_CONFS = Map.of(APOC_EXPORT_FILE_ENABLED, "false",
            APOC_IMPORT_FILE_ALLOW__READ__FROM__FILESYSTEM, "true",
            APOC_IMPORT_FILE_ENABLED, "false",
            APOC_IMPORT_FILE_USE_NEO4J_CONFIG, "true",
            APOC_CONFIG_JOBS_SCHEDULED_NUM_THREADS, "4",
            TRIGGER_REFRESH, "2000",
            APOC_TRIGGER_ENABLED, "false");

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

    @AfterAll
    public void tearDown() {
        db.shutdown();
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
