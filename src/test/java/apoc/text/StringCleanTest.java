package apoc.text;

import apoc.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.Arrays;
import java.util.Collection;

import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static org.junit.Assert.assertEquals;

/**
 * @author Stefan Armbruster
 */
@RunWith(Parameterized.class)
public class StringCleanTest {

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    @BeforeClass
    public static void setUp() throws Exception {
        TestUtil.registerProcedure(db, Strings.class);
    }

    @Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                { "&N[]eo  4 #J-(3.0)  ", "neo4j30"},
                { "German umlaut Ä Ö Ü ä ö ü ß ", "germanumlautaeoeueaeoeuess" },
                { "French çÇéèêëïîôœàâæùûü", "frenchcceeeeiioœaaæuuue"}
        });
    }

    @Parameter(value = 0)
    public String dirty;

    @Parameter(value = 1)
    public String clean;

    @Test
    public void testClean() throws Exception {
        testCall(db,
                "RETURN apoc.text.clean($a) AS value",
                map("a", dirty),
                row -> assertEquals(clean, row.get("value")));
    }

}
