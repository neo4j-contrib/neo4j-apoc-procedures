package apoc.periodic;

import apoc.util.TestUtil;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static apoc.util.TestUtil.testCall;
import static org.junit.Assert.assertEquals;
import static org.neo4j.test.assertion.Assert.assertEventually;

public class PeriodicExtendedTest {

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();

    @Before
    public void initDb() {
        TestUtil.registerProcedure(db, PeriodicExtended.class);
    }

    @Test
    public void testSubmitSchema() {
        testCall(db, "CALL apoc.periodic.submitSchema('subSchema','CREATE INDEX periodicIdx FOR (n:Bar) ON (n.first_name, n.last_name)')",
                (row) -> {
                    assertEquals("subSchema", row.get("name"));
                    assertEquals(false, row.get("done"));
                });

        assertEventually(() -> db.executeTransactionally("SHOW INDEXES YIELD name WHERE name = 'periodicIdx' RETURN count(*) AS count",
                        Collections.emptyMap(),
                        (res) -> res.<Long>columnAs("count").next()),
                val -> val == 1L, 15L, TimeUnit.SECONDS);

        testCall(db, "CALL apoc.periodic.list()", (row) -> {
            assertEquals("subSchema", row.get("name"));
            assertEquals(true, row.get("done"));
        });
    }

    @Test
    public void testSubmitSchemaWithWriteOperation() {
        testCall(db, "CALL apoc.periodic.submitSchema('subSchema','CREATE (:SchemaLabel)')",
                (row) -> {
                    assertEquals("subSchema", row.get("name"));
                    assertEquals(false, row.get("done"));
                });

        assertEventually(() -> db.executeTransactionally("MATCH (n:SchemaLabel) RETURN count(n) AS count",
                        Collections.emptyMap(),
                        (res) -> res.<Long>columnAs("count").next()),
                val -> val == 1L, 15L, TimeUnit.SECONDS);

        testCall(db, "CALL apoc.periodic.list()", (row) -> {
            assertEquals("subSchema", row.get("name"));
            assertEquals(true, row.get("done"));
        });
    }
}
