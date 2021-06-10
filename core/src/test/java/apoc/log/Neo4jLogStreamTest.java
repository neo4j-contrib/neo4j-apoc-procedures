package apoc.log;

import apoc.util.TestUtil;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

import java.nio.file.Paths;
import java.util.stream.Collectors;

import static apoc.ApocConfig.apocConfig;
import static apoc.util.TestUtil.testResult;
import static org.junit.Assert.assertTrue;

public class Neo4jLogStreamTest {
    
    private GraphDatabaseService db;

    @Before
    public void setUp() throws Exception {
        DatabaseManagementService dbManagementService = new TestDatabaseManagementServiceBuilder(
                Paths.get("target").toAbsolutePath()).build();
        apocConfig().setProperty("dbms.directories.logs", "");
        db = dbManagementService.database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME);
        TestUtil.registerProcedure(db, Neo4jLogStream.class);
    }
    
    @Test
    public void testLogStream() {
        testResult(db, "CALL apoc.log.stream('debug.log')", res -> {
            final String wholeFile = Iterators.stream(res.<String>columnAs("line")).collect(Collectors.joining(""));
            assertTrue(wholeFile.contains("apoc.import.file.enabled=false"));
        });
    }
}
