package apoc.export;

import apoc.util.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.io.File;
import java.util.Scanner;

import static apoc.util.MapUtil.map;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author mh
 * @since 22.05.16
 */
public class ExportTest {

    private static final String EXPECTED = "begin\n" +
            "CREATE (:`Foo`:`UNIQUE IMPORT LABEL` {`name`:\"foo\", `UNIQUE IMPORT ID`:0});\n" +
            "CREATE (:`Bar` {`name`:\"bar\", `age`:42});\n" +
            "commit\n" +
            "begin\n" +
            "CREATE INDEX ON :`Foo`(`name`);\n" +
            "CREATE CONSTRAINT ON (node:`Bar`) ASSERT node.`name` IS UNIQUE;\n" +
            "CREATE CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT node.`UNIQUE IMPORT ID` IS UNIQUE;\n" +
            "commit\n" +
            "schema await\n" +
            "begin\n" +
            "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:0}), (n2:`Bar`{`name`:\"bar\"}) CREATE (n1)-[:`KNOWS`]->(n2);\n" +
            "commit\n" +
            "begin\n" +
            "MATCH (n:`UNIQUE IMPORT LABEL`)  WITH n LIMIT 20000 REMOVE n:`UNIQUE IMPORT LABEL` REMOVE n.`UNIQUE IMPORT ID`;\n" +
            "commit\n" +
            "begin\n" +
            "DROP CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT node.`UNIQUE IMPORT ID` IS UNIQUE;\n" +
            "commit";

    private static GraphDatabaseService db;
    private static File directory = new File("target/import");
    static { //noinspection ResultOfMethodCallIgnored
        directory.mkdirs();
    }

    @BeforeClass
    public static void setUp() throws Exception {
        db = new TestGraphDatabaseFactory()
                .newImpermanentDatabaseBuilder()
                .setConfig(GraphDatabaseSettings.load_csv_file_url_root, directory.getAbsolutePath())
                .newGraphDatabase();
        TestUtil.registerProcedure(db, Export.class);
        db.execute("CREATE (f:Foo {name:'foo'})-[:KNOWS]->(b:Bar {name:'bar',age:42})").close();
        db.execute("CREATE INDEX ON :Foo(name)").close();
        db.execute("CREATE CONSTRAINT ON (b:Bar) ASSERT b.name is unique").close();
    }

    @AfterClass
    public static void tearDown() {
        db.shutdown();
    }

    @Test
    public void testExportAllCypher() throws Exception {
        File output = new File(directory, "all.cypher");
        TestUtil.testCall(db, "CALL apoc.export.cypherAll({file},null)",map("file", output.getAbsolutePath()), (r) -> {
            assertEquals(2L, r.get("nodes"));
            assertEquals(1L, r.get("relationships"));
            assertEquals(3L, r.get("properties"));
            assertEquals(output.getAbsolutePath(), r.get("file"));
            assertEquals("database: nodes(2), rels(1)", r.get("source"));
            assertEquals("cypher", r.get("format"));
            assertEquals(true, ((long)r.get("time")) > 0);
//            System.out.println(r);
        });
        Scanner scanner = new Scanner(output).useDelimiter("\\Z");
        String contents = scanner.next();
//        System.out.println(contents);
        assertEquals(EXPECTED,contents);
    }
}
