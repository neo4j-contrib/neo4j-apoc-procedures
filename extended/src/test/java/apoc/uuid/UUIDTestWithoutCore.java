package apoc.uuid;

import apoc.util.TestUtil;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import org.junit.rules.TemporaryFolder;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.ConstraintDefinition;

import java.io.File;

import static apoc.util.TestUtil.waitDbsAvailable;
import static apoc.uuid.UUIDTestUtils.startDbWithUuidApocConfigs;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

public class UUIDTestWithoutCore {
    private static final File directory = new File("target/conf");
    static { //noinspection ResultOfMethodCallIgnored
        directory.mkdirs();
    }

    @ClassRule
    public static TemporaryFolder storeDir = new TemporaryFolder();

    private static GraphDatabaseService db;
    private static DatabaseManagementService databaseManagementService;

    @BeforeClass
    public static void beforeClass() throws Exception {
        databaseManagementService = startDbWithUuidApocConfigs(storeDir);

        db = databaseManagementService.database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME);
        waitDbsAvailable(db);
        TestUtil.registerProcedure(db, Uuid.class);
    }

    @AfterClass
    public static void afterClass() {
        databaseManagementService.shutdown();
    }


    @After
    public void after() throws Exception {
        db.executeTransactionally("CALL apoc.uuid.removeAll()");
        db.executeTransactionally("MATCH (n) DETACH DELETE n");

        // otherwise we can create a GraphDatabaseService db in @Before instead of @BeforeClass
        try (Transaction tx = db.beginTx()) {
            tx.schema().getConstraints().forEach(ConstraintDefinition::drop);
            tx.commit();
        }
    }
    
    @Test(expected = RuntimeException.class)
    public void testAddToAllExistingNodesIfCoreNotInstalled() {
        try {
            // when
            db.executeTransactionally("CREATE CONSTRAINT FOR (p:Person) REQUIRE p.uuid IS UNIQUE");
            db.executeTransactionally("CALL apoc.uuid.install('Person') YIELD label RETURN label");
        } catch (RuntimeException e) {
            // then
            Throwable except = ExceptionUtils.getRootCause(e);
            assertTrue(except instanceof RuntimeException);
            assertEquals("apoc core needs to be installed when using apoc.uuid.install with the flag addToExistingNodes = true", except.getMessage());
            throw e;
        }
    }
}
