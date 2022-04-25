package apoc.log;

import apoc.util.TestUtil;
import org.junit.Before;
import org.junit.After;
import org.junit.Test;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

import java.nio.file.Paths;
import java.util.UUID;
import java.util.stream.Collectors;

import static apoc.ApocConfig.apocConfig;
import static apoc.util.TestUtil.testResult;
import static org.junit.Assert.assertTrue;

public class Neo4jLogStreamTest {
    
    private GraphDatabaseService db;
    private DatabaseManagementService dbManagementService;

    @Before
    public void setUp() throws Exception {
        dbManagementService = new TestDatabaseManagementServiceBuilder(
                Paths.get("target", UUID.randomUUID().toString()).toAbsolutePath()).build();
        apocConfig().setProperty("dbms.directories.logs", "");
        db = dbManagementService.database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME);
        TestUtil.registerProcedure(db, Neo4jLogStream.class);
    }

    @After
    public void tearDown() {
        dbManagementService.shutdown();
    }

    @Test
    public void testLogStream() {
        testResult(db, "CALL apoc.log.stream('debug.log')", res -> {
            final String wholeFile = Iterators.stream(res.<String>columnAs("line")).collect(Collectors.joining(""));
            assertTrue(wholeFile.contains("apoc.import.file.enabled=false"));
        });
    }
}
