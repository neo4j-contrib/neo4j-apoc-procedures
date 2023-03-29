package apoc.uuid;

import apoc.periodic.Periodic;
import apoc.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

import java.io.IOException;

import static apoc.util.TestUtil.waitDbsAvailable;
import static apoc.uuid.UUIDTestUtils.*;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

public class UUIDRestartTest {

    @Rule
    public TemporaryFolder storeDir = new TemporaryFolder();

    private GraphDatabaseService db;
    private GraphDatabaseService sysDb;
    private DatabaseManagementService databaseManagementService;

    @Before
    public void setUp() throws IOException {
        databaseManagementService = startDbWithUuidApocConfigs(storeDir);

        db = databaseManagementService.database(DEFAULT_DATABASE_NAME);
        sysDb = databaseManagementService.database(SYSTEM_DATABASE_NAME);
        waitDbsAvailable(db, sysDb);
        TestUtil.registerProcedure(db, UUIDNewProcedures.class, Uuid.class, Periodic.class);
    }

    @After
    public void tearDown() {
        databaseManagementService.shutdown();
    }

    private void restartDb() {
        databaseManagementService.shutdown();
        databaseManagementService = new TestDatabaseManagementServiceBuilder(storeDir.getRoot().toPath()).build();
        db = databaseManagementService.database(DEFAULT_DATABASE_NAME);
        sysDb = databaseManagementService.database(SYSTEM_DATABASE_NAME);
        waitDbsAvailable(db, sysDb);
        TestUtil.registerProcedure(db, UUIDNewProcedures.class, Uuid.class, Periodic.class);
    }

    @Test
    public void testSetupUuidRunsAfterRestart() {
        sysDb.executeTransactionally("CALL apoc.uuid.setup('Person')");
        awaitUuidDiscovered(db, "Person");

        db.executeTransactionally("CREATE (p:Person {id: 1})");
        TestUtil.testCall(db, "MATCH (n:Person) RETURN n.uuid AS uuid",
                row -> assertIsUUID(row.get("uuid"))
        );

        restartDb();

        db.executeTransactionally("CREATE (p:Person {id:2})");
        TestUtil.testCall(db, "MATCH (n:Person{id:1}) RETURN n.uuid AS uuid",
                r -> assertIsUUID(r.get("uuid"))
        );
        TestUtil.testCall(db, "MATCH (n:Person{id:2}) RETURN n.uuid AS uuid",
                r -> assertIsUUID(r.get("uuid"))
        );

    }
}
