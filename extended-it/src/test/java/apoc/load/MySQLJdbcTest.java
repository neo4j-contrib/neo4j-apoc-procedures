package apoc.load;

import apoc.util.s3.MySQLContainerExtension;
import apoc.util.TestUtil;
import apoc.util.Util;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.Map;

import static apoc.util.TestUtil.testCall;
import static org.junit.Assert.assertEquals;

public class MySQLJdbcTest extends AbstractJdbcTest {

    @ClassRule
    public static MySQLContainerExtension mysql = new MySQLContainerExtension();

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    @BeforeClass
    public static void setUpContainer() {
        mysql.start();
        TestUtil.registerProcedure(db, Jdbc.class);
    }

    @AfterClass
    public static void tearDown() {
        mysql.stop();
        db.shutdown();
    }

    @Test
    public void testLoadJdbc() {
        testCall(db, "CALL apoc.load.jdbc($url, $table, [])",
                Util.map(
                        "url", mysql.getJdbcUrl(),
                        "table", "country"),
                row -> {
                    Map<String, Object> expected = Util.map(
                            "Code", "NLD",
                            "Name", "Netherlands",
                            "Continent", "Europe",
                            "Region", "Western Europe",
                            "SurfaceArea", 41526f,
                            "IndepYear", 1581,
                            "Population", 15864000,
                            "LifeExpectancy", 78.3f,
                            "GNP", 371362f,
                            "GNPOld", 360478f,
                            "LocalName", "Nederland",
                            "GovernmentForm", "Constitutional Monarchy",
                            "HeadOfState", "Beatrix",
                            "Capital", 5,
                            "Code2", "NL");
                    assertEquals(expected, row.get("row"));
                });
    }
}
