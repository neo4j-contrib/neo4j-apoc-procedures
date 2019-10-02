package apoc.label;

import apoc.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import static apoc.util.TestUtil.testCall;
import static org.junit.Assert.assertEquals;

public class LabelTest {

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    @BeforeClass
    public static void setUp() throws Exception {
        TestUtil.registerProcedure(db, Label.class);
    }

    @Test
    public void testVerifyNodeLabelExistence() throws Exception {

        db.executeTransactionally("create (a:Person{name:'Foo'})");

        testCall(db, "MATCH (a) RETURN apoc.label.exists(a, 'Person') as value",
                (row) -> {
                    assertEquals(true, row.get("value"));
                });
        testCall(db, "MATCH (a) RETURN apoc.label.exists(a, 'Dog') as value",
                (row) -> {
                    assertEquals(false, row.get("value"));
                });
    }

    @Test
    public void testVerifyRelTypeExistence() throws Exception {

        db.executeTransactionally("create (a:Person{name:'Foo'}), (b:Person{name:'Bar'}), (a)-[:LOVE{since:2010}]->(b)");

        testCall(db, "MATCH ()-[a]->() RETURN apoc.label.exists(a, 'LOVE') as value",
                (row) -> {
                    assertEquals(true, row.get("value"));
                });
        testCall(db, "MATCH ()-[a]->() RETURN apoc.label.exists(a, 'LIVES_IN') as value",
                (row) -> {
                    assertEquals(false, row.get("value"));
                });

    }
}
