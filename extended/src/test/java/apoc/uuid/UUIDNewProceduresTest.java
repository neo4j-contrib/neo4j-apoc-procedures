package apoc.uuid;

import apoc.create.Create;
import apoc.periodic.Periodic;
import apoc.util.TestUtil;
import apoc.util.Util;
import org.hamcrest.Matchers;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.ConstraintDefinition;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static apoc.ApocConfig.apocConfig;
import static apoc.util.SystemDbTestUtil.*;
import static apoc.util.SystemDbUtil.*;
import static apoc.util.TestUtil.*;
import static apoc.uuid.UUIDNewProcedures.UUID_NOT_SET;
import static apoc.uuid.UUIDTest.UUID_TEST_REGEXP;
import static apoc.uuid.UUIDTest.assertResult;
import static apoc.uuid.UUIDTestUtils.*;
import static apoc.uuid.UuidConfig.*;
import static apoc.uuid.UuidHandler.APOC_UUID_REFRESH;
import static java.util.Collections.emptyMap;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.*;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.internal.helpers.collection.MapUtil.map;
import static org.neo4j.test.assertion.Assert.assertEventually;


public class UUIDNewProceduresTest {
    private static final File directory = new File("target/conf");
    static { //noinspection ResultOfMethodCallIgnored
        directory.mkdirs();
    }

    @ClassRule
    public static TemporaryFolder storeDir = new TemporaryFolder();

    private static GraphDatabaseService sysDb;
    private static GraphDatabaseService db;
    private static DatabaseManagementService databaseManagementService;

    @BeforeClass
    public static void beforeClass() throws Exception {
        databaseManagementService = startDbWithUuidApocConfigs(storeDir);

        db = databaseManagementService.database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME);
        sysDb = databaseManagementService.database(GraphDatabaseSettings.SYSTEM_DATABASE_NAME);
        waitDbsAvailable(db, sysDb);
        TestUtil.registerProcedure(db, Uuid.class, UUIDNewProcedures.class, Periodic.class, Create.class);
    }

    @AfterClass
    public static void afterClass() {
        databaseManagementService.shutdown();
    }

    @After
    public void after() throws Exception {
        sysDb.executeTransactionally("CALL apoc.uuid.dropAll");
        testCallCountEventually(db, "CALL apoc.uuid.list", 0, TIMEOUT);
        db.executeTransactionally("MATCH (n) DETACH DELETE n");

        // otherwise we can create a GraphDatabaseService db in @Before instead of @BeforeClass
        try (Transaction tx = db.beginTx()) {
            tx.schema().getConstraints().forEach(ConstraintDefinition::drop);
            tx.commit();
        }
    }

    //
    // test cases taken and adapted from UUIDTest.java
    //

    @Test
    public void testUUID() {
        // given
        sysDb.executeTransactionally("CALL apoc.uuid.setup('Person')");
        UUIDTestUtils.awaitUuidDiscovered(db, "Person");

        // when
        db.executeTransactionally("CREATE (p:Person{name:'Daniel'})-[:WORK]->(c:Company{name:'Neo4j'})");

        // then
        try (Transaction tx = db.beginTx()) {
            Node company = (Node) tx.execute("MATCH (c:Company) return c").next().get("c");
            assertFalse(company.hasProperty("uuid"));
            Node person = (Node) tx.execute("MATCH (p:Person) return p").next().get("p");
            assertTrue(person.getAllProperties().containsKey("uuid"));

            assertTrue(person.getAllProperties().get("uuid").toString().matches(UUID_TEST_REGEXP));
            tx.commit();
        }
    }

    @Test
    public void testUUIDWithSetLabel() {
        // given
        sysDb.executeTransactionally("CALL apoc.uuid.setup('Mario', 'neo4j', {addToSetLabels: true}) YIELD label RETURN label");
        awaitUuidDiscovered(db, "Mario", DEFAULT_UUID_PROPERTY, true);

        // when
        db.executeTransactionally("CREATE (p:Luigi {foo:'bar'}) SET p:Mario");
        // then
        TestUtil.testCall(db, "MATCH (a:Luigi:Mario) RETURN a.uuid as uuid",
                row -> assertIsUUID(row.get("uuid")));

        // - set after creation
        db.executeTransactionally("CREATE (:Peach)");
        // when
        db.executeTransactionally("MATCH (p:Peach) SET p:Mario");
        // then
        TestUtil.testCall(db, "MATCH (a:Peach:Mario) RETURN a.uuid as uuid",
                row -> assertIsUUID(row.get("uuid")));

        TestUtil.testCall(sysDb, "CALL apoc.uuid.drop('Mario')",
                (row) -> assertResult(row, "Mario", false,
                        Util.map(UUID_PROPERTY_KEY, "uuid", ADD_TO_SET_LABELS_KEY, true)));
    }

    @Test
    public void testUUIDWithoutRemovedUuid() {
        // given
        sysDb.executeTransactionally("CALL apoc.uuid.setup('TestSetUuid') YIELD label RETURN label");
        UUIDTestUtils.awaitUuidDiscovered(db, "TestSetUuid");

        String expectedUuid = "dab404ee-391d-11e9-b210-d663bd873d93";
        // when
        db.executeTransactionally("CREATE (n:TestSetUuid {name:'TestSetUuid', uuid: $uuid})",
                Map.of("uuid", expectedUuid));

        // then
        try (Transaction tx = db.beginTx()) {
            Node n = (Node) tx.execute("MATCH (n:TestSetUuid) return n").next().get("n");
            assertTrue(n.getAllProperties().containsKey("uuid"));
            assertEquals(expectedUuid, n.getProperty("uuid")); // Check if the uuid if the same when created
            tx.commit();
        }
    }

    @Test
    public void testUUIDSetUuidToEmptyAndRestore() {
        // given
        sysDb.executeTransactionally("CALL apoc.uuid.setup('TestEmptyRestore') YIELD label RETURN label");
        UUIDTestUtils.awaitUuidDiscovered(db, "TestEmptyRestore");

        String expectedUuid = "dab404ee-391d-11e9-b210-d663bd873d93";
        db.executeTransactionally("CREATE (n:TestEmptyRestore {name:'test', uuid: $uuid})",
                Map.of("uuid", expectedUuid));

        // when
        db.executeTransactionally("MATCH (t:TestEmptyRestore) SET t.uuid = ''");

        // then
        try (Transaction tx = db.beginTx()) {
            Node n = (Node) tx.execute("MATCH (n:TestEmptyRestore) return n").next().get("n");
            assertTrue(n.getAllProperties().containsKey("uuid"));
            assertEquals(expectedUuid, n.getProperty("uuid"));
            tx.commit();
        }
    }

    @Test
    public void testUUIDDeleteUuidAndRestore() {
        // given
        sysDb.executeTransactionally("CALL apoc.uuid.setup('TestRestoreDeleted') YIELD label RETURN label");
        UUIDTestUtils.awaitUuidDiscovered(db, "TestRestoreDeleted");
        db.executeTransactionally("CREATE (n:TestRestoreDeleted {name:'test', uuid:'dab404ee-391d-11e9-b210-d663bd873d93'})");

        // when
        db.executeTransactionally("MATCH (t:TestRestoreDeleted) remove t.uuid");

        // then
        try (Transaction tx = db.beginTx()) {
            Node n = (Node) tx.execute("MATCH (n:TestRestoreDeleted) return n").next().get("n");
            assertTrue(n.getAllProperties().containsKey("uuid"));
            assertEquals("dab404ee-391d-11e9-b210-d663bd873d93", n.getProperty("uuid"));
            tx.commit();
        }
    }

    @Test
    public void testUUIDSetUuidToEmpty() {
        // given
        sysDb.executeTransactionally("CALL apoc.uuid.setup('TestToEmpty') YIELD label RETURN label");
        UUIDTestUtils.awaitUuidDiscovered(db, "TestToEmpty");

        db.executeTransactionally("CREATE (n:TestToEmpty {name:'empty'})");

        // when
        db.executeTransactionally("MATCH (t:TestToEmpty) SET t.uuid = ''");

        // then
        testCall(db, "MATCH (n:TestToEmpty) return n.uuid AS uuid",
                (row) -> assertIsUUID(row.get("uuid"))
        );
    }

    @Test
    public void testUUIDList() {
        // when
        sysDb.executeTransactionally("CALL apoc.uuid.setup('Bar') YIELD label RETURN label");
        UUIDTestUtils.awaitUuidDiscovered(db, "Bar");

        // then
        TestUtil.testCall(db, "CALL apoc.uuid.list()",
                (row) -> assertResult(row, "Bar", true,
                        Util.map(UUID_PROPERTY_KEY, "uuid", ADD_TO_SET_LABELS_KEY, false)));
    }

    @Test
    public void testUUIDListAddToExistingNodes() {
        // given
        db.executeTransactionally("UNWIND Range(1,10) as i CREATE(bar:Bar{id: i})");

        // when
        sysDb.executeTransactionally("CALL apoc.uuid.setup('Bar')");
        UUIDTestUtils.awaitUuidDiscovered(db, "Bar");

        assertEventually(() -> {
            List<String> uuidList = TestUtil.firstColumn(db, "MATCH (n:Bar) RETURN n.uuid AS uuid");
            assertEquals(10, uuidList.size());
            return uuidList.stream()
                    .allMatch(uuid -> uuid != null && uuid.matches(UUID_TEST_REGEXP));
        }, (val) -> val, TIMEOUT, TimeUnit.SECONDS);
    }

    @Test
    public void testConstraintAutoCreation() {
        sysDb.executeTransactionally("CALL apoc.uuid.setup('Foo') YIELD label RETURN label");
        sysDb.executeTransactionally("CALL apoc.uuid.setup('Bar', 'neo4j', {uuidProperty: 'foo'}) YIELD label RETURN label");

        // one uuid with a constraint already created
        db.executeTransactionally("CREATE CONSTRAINT baz_uuid FOR (test:Baz) REQUIRE test.another IS UNIQUE");
        sysDb.executeTransactionally("CALL apoc.uuid.setup('Baz', 'neo4j', {uuidProperty: 'another'}) YIELD label RETURN label");

        testCallCountEventually(db, "CALL apoc.uuid.list", 3, TIMEOUT);
        // check constraint auto-creation
        testResult(db, "SHOW CONSTRAINTS YIELD name, type, labelsOrTypes, properties ORDER BY labelsOrTypes", res -> {
            Map<String, Object> row = res.next();
            constraintAssertions(row, "Bar", "foo");

            row = res.next();
            constraintAssertions(row, "Baz", "another");
            assertEquals("baz_uuid" , row.get("name"));

            row = res.next();
            constraintAssertions(row, "Foo", "uuid");

            assertFalse(res.hasNext());
        });
    }

    private static void constraintAssertions(Map<String, Object> r, String label, String prop) {
        assertEquals(List.of(label) , r.get("labelsOrTypes"));
        assertEquals(List.of(prop) , r.get("properties"));
        assertEquals("UNIQUENESS" , r.get("type"));
    }

    @Test
    public void testSetupAndDropUuid() {
        // when
        sysDb.executeTransactionally("CALL apoc.uuid.setup('Test', 'neo4j', {uuidProperty: 'foo'}) YIELD label RETURN label");
        awaitUuidDiscovered(db, "Test", "foo", DEFAULT_ADD_TO_SET_LABELS);

        testCall(db, "CALL apoc.uuid.list()",
                (row) -> assertResult(row, "Test", true,
                        Util.map(UUID_PROPERTY_KEY, "foo", ADD_TO_SET_LABELS_KEY, false)));
        testCall(sysDb, "CALL apoc.uuid.drop('Test')",
                (row) -> assertResult(row, "Test", false,
                        Util.map(UUID_PROPERTY_KEY, "foo", ADD_TO_SET_LABELS_KEY, false)));
    }

    @Test
    public void testDropNotExistingUuid() {
        testCallEmpty(sysDb, "CALL apoc.uuid.drop('Test')", emptyMap());
    }

    @Test
    public void testDropAllWithoutUuids() {
        testCallEmpty(sysDb, "CALL apoc.uuid.dropAll", emptyMap());
    }

    @Test
    public void testNotAddToExistingNodes() {
        // given
        db.executeTransactionally("CREATE (d:Person {name:'Daniel'})-[:WORK]->(l:Company {name:'Neo4j'})");

        // when
        sysDb.executeTransactionally("CALL apoc.uuid.setup('Person', 'neo4j', {addToExistingNodes: false}) YIELD label RETURN label");
        UUIDTestUtils.awaitUuidDiscovered(db, "Person");

        // then
        try (Transaction tx = db.beginTx()) {
            Node n = (Node) tx.execute("MATCH (person:Person) return person").next().get("person");
            assertFalse(n.getAllProperties().containsKey("uuid"));
            tx.commit();
        }
    }

    @Test
    public void testAddToExistingNodes() {
        // given
        db.executeTransactionally("CREATE (d:Person {name:'Daniel'})-[:WORK]->(l:Company {name:'Neo4j'})");

        // when
        sysDb.executeTransactionally("CALL apoc.uuid.setup('Person') YIELD label RETURN label");
        UUIDTestUtils.awaitUuidDiscovered(db, "Person");

        // then
        try (Transaction tx = db.beginTx()) {
            Node n = (Node) tx.execute("MATCH (person:Person) return person").next().get("person");
            assertTrue(n.getAllProperties().containsKey("uuid"));
            assertIsUUID(n.getAllProperties().get("uuid").toString());
            tx.commit();
        }
    }

    @Test
    public void testDropAllUuid() {
        // given
        sysDb.executeTransactionally("CALL apoc.uuid.setup('Bar') YIELD label RETURN label");
        sysDb.executeTransactionally("CALL apoc.uuid.setup('Test', 'neo4j', {addToExistingNodes: false, uuidProperty: 'foo'}) YIELD label RETURN label");
        testCallCountEventually(db, "CALL apoc.uuid.list", 2, TIMEOUT);

        // when
        TestUtil.testResult(sysDb, "CALL apoc.uuid.dropAll",
                (result) -> {
                    // then
                    Map<String, Object> row = result.next();
                    assertResult(row, "Bar", false,
                            Util.map(UUID_PROPERTY_KEY, "uuid", ADD_TO_SET_LABELS_KEY, false));
                    row = result.next();
                    assertResult(row, "Test", false,
                            Util.map(UUID_PROPERTY_KEY, "foo", ADD_TO_SET_LABELS_KEY, false));
                });

        testCallCountEventually(db, "CALL apoc.uuid.list", 0, TIMEOUT);
    }

    //
    // new test cases
    //

    @Test
    public void testUuidShow() {
        String label1 = "Show1";
        String label2 = "Show2";

        testCall(sysDb, "CALL apoc.uuid.setup($name)",
                map("name", label1),
                r -> assertEquals(label1, r.get("label")));

        testCall(sysDb, "CALL apoc.uuid.setup($name)",
                map("name", label2),
                r -> assertEquals(label2, r.get("label")));

        // not updated
        testResult(db, "CALL apoc.uuid.show('neo4j')",
                res -> {
                    Map<String, Object> row = res.next();
                    assertEquals(label1, row.get("label"));
                    Map<String, Object> defaultProperties = Map.of(UUID_PROPERTY_KEY, DEFAULT_UUID_PROPERTY,
                            ADD_TO_SET_LABELS_KEY, DEFAULT_ADD_TO_SET_LABELS);
                    assertEquals(defaultProperties, row.get("properties"));
                    row = res.next();
                    assertEquals(label2, row.get("label"));
                    assertEquals(defaultProperties, row.get("properties"));
                    Assert.assertFalse(res.hasNext());
                });
    }

    @Test
    public void testSameNodeWithMultipleUuid() {
        String label1 = "Show1";
        String label2 = "Show2";

        testCall(sysDb, "CALL apoc.uuid.setup($name, 'neo4j', $config)",
                Map.of("name", label1,
                        "config", Map.of(ADD_TO_SET_LABELS_KEY, true)
                ),
                r -> assertEquals(label1, r.get("label")));

        testCall(sysDb, "CALL apoc.uuid.setup($name, 'neo4j', $config)",
                Map.of("name", label2,
                        "config", Map.of(ADD_TO_SET_LABELS_KEY, true)
                ),
                r -> assertEquals(label2, r.get("label")));

        testCallCountEventually(db, "CALL apoc.uuid.list", 2, TIMEOUT);

        db.executeTransactionally("CREATE (n:Show1)");

        final String[] uuid = new String[1];
        testCall(db, "MATCH (c:Show1) RETURN c.uuid as uuid",
                (row) -> {
                    uuid[0] = (String) row.get("uuid");
                    assertIsUUID(uuid[0]);
                }
        );

        // check that the uuid value remains unchanged
        db.executeTransactionally("MATCH (n:Show1) SET n:Show2");
        testCall(db, "MATCH (c:Show1:Show2) RETURN c.uuid as uuid",
                (row) -> assertEquals(uuid[0], row.get("uuid"))
        );
    }

    @Test
    public void testSetupUuidInUserDb() {
        try {
            testCall(db, "CALL apoc.uuid.setup('AnotherLabel')",
                    r -> fail("Should fail because of user db execution"));
        } catch (QueryExecutionException e) {
            assertThat(e.getMessage(), Matchers.containsString(PROCEDURE_NOT_ROUTED_ERROR));
        }
    }

    @Test
    public void testSetupUuidInWrongDb() {
        String dbNotExistent = "notExistent";
        try {
            testCall(sysDb, "CALL apoc.uuid.setup('notExistent', $db)",
                    Map.of("db", dbNotExistent),
                    r -> fail("Should fail because of database not found"));
        } catch (QueryExecutionException e) {
            String expected = String.format("The user database with name '%s' does not exist", dbNotExistent);
            assertThat(e.getMessage(), Matchers.containsString(expected));
        }
    }

    @Test
    public void testSetupUuidInSystemDb() {
        try {
            testCall(sysDb, "CALL apoc.uuid.setup('myName', 'system')",
                    r -> fail("Should fail because of system db pointing"));
        } catch (RuntimeException e) {
            assertThat(e.getMessage(), Matchers.containsString(BAD_TARGET_ERROR));
        }
    }


    @Test
    public void testEventualConsistencyWithMultipleListeners() {

        final String label = "EventualLabel";


        // this does nothing, just to test consistency with multiple uuids
        sysDb.executeTransactionally("CALL apoc.uuid.setup($label)",
                map("label", label) );

        // create uuid
        sysDb.executeTransactionally("CALL apoc.uuid.setup($label)",
                map("label", label));
        UUIDTestUtils.awaitUuidDiscovered(db, label);

        // check uuid
        db.executeTransactionally("CREATE (n:EventualLabel)");
        testCall(db, "MATCH (c:EventualLabel) RETURN c.uuid AS uuid",
                (row) -> assertIsUUID(row.get("uuid"))
        );

        // this does nothing, just to test consistency with multiple uuids
        String labelTwo = "EventualLabelTwo";
        sysDb.executeTransactionally("CALL apoc.uuid.setup($label)",
                map("label", labelTwo) );
        UUIDTestUtils.awaitUuidDiscovered(db, labelTwo);
        testCallCount(db, "CALL apoc.uuid.list", 2);

        // check uuid
        db.executeTransactionally("CREATE (n:EventualLabelTwo)");
        testCall(db, "MATCH (c:EventualLabelTwo) RETURN c.uuid as uuid",
                (row) -> assertIsUUID(row.get("uuid"))
        );

        // check uuids
        db.executeTransactionally("CREATE (n:EventualLabel {id: 2})");
        testCall(db, "MATCH (c:EventualLabel {id: 2}) RETURN c.uuid as uuid",
                (row) -> assertIsUUID(row.get("uuid"))
        );
    }

    // to check that with new procedures like apoc.uuid.setup
    // we have to set `apoc.uuid.refresh`
    @Test
    public void testUuidRefreshNotSet() {
        apocConfig().setProperty(APOC_UUID_REFRESH, null);
        try {
            testCall(sysDb, "CALL apoc.uuid.setup('AnotherLabel')",
                    r -> fail("Should fail because apoc.uuid.refresh is not set"));
        } catch (RuntimeException e) {
            assertThat(e.getMessage(), Matchers.containsString(UUID_NOT_SET));
        }
        apocConfig().setProperty(APOC_UUID_REFRESH, PROCEDURE_DEFAULT_REFRESH);
    }
}
