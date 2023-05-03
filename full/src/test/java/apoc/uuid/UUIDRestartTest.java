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
        databaseManagementService = startDbWithUuidApocConfs(storeDir);

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
