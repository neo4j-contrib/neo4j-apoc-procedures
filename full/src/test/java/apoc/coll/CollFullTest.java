package apoc.coll;

import apoc.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;
import org.neo4j.values.storable.DurationValue;

import static org.junit.Assert.assertEquals;

public class CollFullTest {

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();
    
    @BeforeClass
    public static void setUp() throws Exception {
        TestUtil.registerProcedure(db, CollFull.class);
    }

    @Test
    public void testAvgDuration() {
        TestUtil.testCall(db, "WITH [duration('P2DT4H1S'), duration('PT1H1S'), duration('PT1H6S'), duration('PT1H5S')] AS dur " +
                        "RETURN apoc.coll.avgDuration(dur) AS value",
                (row) -> {
                    final Object value = row.get("value");
                    assertEquals(DurationValue.parse("PT13H45M3.25S"), value);
                });
    }
}
