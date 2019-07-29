package apoc.algo;

import apoc.util.TestUtil;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import static apoc.util.TestUtil.testCall;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;

public class LabelPropagationTest {

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();

    @Before
   	public void setUp() throws Exception {
   		TestUtil.registerProcedure(db, LabelPropagation.class);
   	}

    @Test
    public void testCommunity() {
        db.execute("CREATE (n {id: 0, partition: 1}) " +
                   "CREATE (n)-[:X]->({id: 1, weight: 1.0, partition: 1})" +
                   "CREATE (n)-[:X]->({id: 2, weight: 2.0, partition: 1})" +
                   "CREATE (n)-[:X]->({id: 3, weight: 1.0, partition: 1})" +
                   "CREATE (n)-[:X]->({id: 4, weight: 1.0, partition: 1})" +
                   "CREATE (n)-[:X]->({id: 5, weight: 8.0, partition: 2})"
        ).close();

        db.execute("CALL apoc.algo.community(1,null,'partition','X','OUTGOING','weight',1)").close();
        testCall(
            db,
            "MATCH (n) WHERE n.id = 0 RETURN n.partition AS partition",
            (r) -> assertThat(r.get("partition"), equalTo(2L))
        );
    }
}
