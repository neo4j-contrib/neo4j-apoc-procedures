package apoc.util;

import org.junit.Assume;
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

    public static String checkEnvVar(String envKey) {
        String value = System.getenv(envKey);
        Assume.assumeNotNull("No %s environment configured".formatted(envKey), value);
        return value;
    }
}
