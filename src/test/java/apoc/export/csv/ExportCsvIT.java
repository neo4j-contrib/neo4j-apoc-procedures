package apoc.export.csv;

import apoc.util.Neo4jContainerExtension;
import apoc.util.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.driver.v1.Session;

import java.util.List;

import static apoc.util.MapUtil.map;
import static apoc.util.TestContainerUtil.*;
import static apoc.util.TestUtil.isTravis;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeNotNull;

/**
 * @author as
 * @since 13.02.19
 */
public class ExportCsvIT {

    private static Neo4jContainerExtension neo4jContainer;
    private static Session session;

    @BeforeClass
    public static void beforeAll() {
        assumeFalse(isTravis());
        TestUtil.ignoreException(() -> {
            executeGradleTasks("clean", "shadow");
            neo4jContainer = createEnterpriseDB(true);
            neo4jContainer.start();
        }, Exception.class);
        assumeNotNull(neo4jContainer);
        session = neo4jContainer.getSession();
    }

    @AfterClass
    public static void afterAll() {
        if (neo4jContainer != null) {
            neo4jContainer.close();
        }
        cleanBuild();
    }

    @Test
    public void testExportQueryCsvIssue1188() throws Exception {
        String copyright = "\n" +
                "(c) 2018 Hovsepian, Albanese, et al. \"\"ASCB(r),\"\" \"\"The American Society for Cell Biology(r),\"\" and \"\"Molecular Biology of the Cell(r)\"\" are registered trademarks of The American Society for Cell Biology.\n" +
                "2018\n" +
                "\n" +
                "This article is distributed by The American Society for Cell Biology under license from the author(s). Two months after publication it is available to the public under an Attribution-Noncommercial-Share Alike 3.0 Unported Creative Commons License.\n" +
                "\n";
        String pk = "5921569";
        session.writeTransaction(tx -> tx.run("CREATE (n:Document{pk:{pk}, copyright: {copyright}})", map("copyright", copyright, "pk", pk)));
        String query = "MATCH (n:Document{pk:'5921569'}) return n.pk as pk, n.copyright as copyright";
        testCall(session, "CALL apoc.export.csv.query({query}, null, {config})", map("query", query,
                "config", map("stream", true)),
                (r) -> {
                    List<String[]> csv = CsvTestUtil.toCollection(r.get("data").toString());
                    assertEquals(2, csv.size());
                    assertArrayEquals(new String[]{"pk","copyright"}, csv.get(0));
                    assertArrayEquals(new String[]{"5921569",copyright}, csv.get(1));
                });
        session.writeTransaction(tx -> tx.run("MATCH (d:Document) DETACH DELETE d"));
    }

}