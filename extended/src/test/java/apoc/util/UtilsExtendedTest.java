package apoc.util;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import static apoc.util.TestUtil.testCall;
import static org.junit.Assert.assertTrue;

public class UtilsExtendedTest {

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    @BeforeClass
    public static void setUp() {
        TestUtil.registerProcedure(db, UtilsExtended.class);
    }

    @Test
    public void testMultipleCharsetsCompressionWithDifferentResults() {
        testCall(db, "RETURN apoc.util.hashCode(rand()) AS hashCode", 
                r -> assertTrue(r.get("hashCode") instanceof Long)
        );
    }
}
