package apoc.text;

import apoc.util.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.TestGraphDatabaseFactory;

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

    private static GraphDatabaseService db;


    @BeforeClass
    public static void setUp() throws Exception {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        TestUtil.registerProcedure(db, Strings.class);
    }

    @AfterClass
    public static void tearDown() {
        db.shutdown();
    }

    @Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                { "&N[]eo  4 #J-(3.0)  ", "neo4j30"},
                { "German umlaut Ä Ö Ü ä ö ü ß ", "germanumlautaeoeueaeoeuess" },
                { "French çÇéèêëïîôœàâæùûü", "frenchcceeeeiioaauuue"}
        });
    }

    @Parameter(value = 0)
    public String dirty;

    @Parameter(value = 1)
    public String clean;

    @Test
    public void testClean() throws Exception {
        testCall(db,
                "CALL apoc.text.clean({a})",
                map("a", dirty),
                row -> assertEquals(clean, row.get("value")));
    }

}
