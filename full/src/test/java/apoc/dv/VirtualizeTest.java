package apoc.dv;

import apoc.ApocSettings;
import apoc.create.Create;
import apoc.load.AbstractJdbcTest;
import apoc.load.Jdbc;
import apoc.load.LoadCsv;
import apoc.periodic.Periodic;
import apoc.util.TestUtil;
import org.junit.*;
import org.junit.rules.TestName;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static apoc.ApocConfig.apocConfig;
import static apoc.util.TestUtil.*;
import static org.junit.Assert.assertEquals;

public class VirtualizeTest extends AbstractJdbcTest{

    public static final Label PERSON = Label.label("Person");

    private Connection conn;

    @Rule
    public TestName testName = new TestName();

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(ApocSettings.apoc_import_file_enabled, true);

    private static final String TEST_WITH_AUTHENTICATION = "WithAuthentication";

    @Before
    public void setUp() throws Exception {
        apocConfig().setProperty("apoc.jdbc.derby.url","jdbc:derby:derbyDB");
        apocConfig().setProperty("apoc.jdbc.test.sql","SELECT * FROM PERSON");
        apocConfig().setProperty("apoc.jdbc.testparams.sql","SELECT * FROM PERSON WHERE NAME = ?");
        TestUtil.registerProcedure(db, Virtualize.class, Jdbc.class);
        createPersonTableAndData();
    }

    @After
    public void tearDown() throws SQLException {
        conn.close();
        try {
            if (testName.getMethodName().endsWith(TEST_WITH_AUTHENTICATION)) {
                DriverManager.getConnection("jdbc:derby:derbyDB;user=apoc;password=Ap0c!#Db;shutdown=true");
            } else {
                DriverManager.getConnection("jdbc:derby:derbyDB;shutdown=true");
            }
        } catch (SQLException e) {
            // DerbyDB shutdown always raise a SQLException, see: http://db.apache.org/derby/docs/10.14/devguide/tdevdvlp20349.html
            if (((e.getErrorCode() == 45000)
                    && ("08006".equals(e.getSQLState())))) {
                // Note that for single database shutdown, the expected
                // SQL state is "08006", and the error code is 45000.
            } else {
                throw e;
            }
        }
        System.clearProperty("derby.connection.requireAuthentication");
        System.clearProperty("derby.user.apoc");
    }

    @Test
    public void testVirtualizeCSV() throws Exception {

        String vrName = "csv_vr";
        String url = getUrlFileName("test.csv").toString();
        String desc = "person's details";


        testCall(db, "call apoc.dv.catalog.add(\"" + vrName + "\",\n" +
                        "                    { vrType: \"CSV\", url: \"" + url + "\", " +
                        " query: \" row.name = {vrp:name} and  row.age starts with {vrp:age} \", " +
                        " desc: \"" + desc + "\", labels:[\"Person\"], header: true })",
                (row) -> {
                    //Node node = (Node) row.get("node");
                    assertEquals(vrName, row.get("name"));
                    assertEquals(url, row.get("URL"));
                    assertEquals("CSV", row.get("type"));
                    assertEquals(new ArrayList<>(List.of("Person")), row.get("labels"));
                    assertEquals(desc, row.get("desc"));
                });

        ArrayList<Label> personTypes = new ArrayList<>(List.of(Label.label("Person")));
        ArrayList<String> personTypesAsStrings = new ArrayList<>(List.of("Person"));

        testCall(db, "call apoc.dv.catalog.list() ",
                (result) -> {
                    assertEquals(vrName, result.get("name"));
                    assertEquals(url, result.get("URL"));
                    assertEquals("CSV", result.get("type"));
                    assertEquals(personTypes, result.get("labels"));
                    assertEquals(desc, result.get("desc"));
                });

        String personName = "Rana";
        String personAge = "11";

        testCall(db, "call apoc.dv.query('" + vrName + "' , { name: '" + personName + "', age: '"  + personAge + "' })",
                (row) -> {
                    Node node = (Node) row.get("node");
                    assertEquals(personName, node.getProperty("name"));
                    assertEquals(personAge, node.getProperty("age"));
                    assertEquals(personTypes, node.getLabels());
                });

    }

    @Test
    public void testVirtualizeJDBC() throws Exception {

        String name = "jdbc_vr";
        String url = "jdbc:derby:derbyDB";
        String desc = "person's details";
        ArrayList<String> personTypes = new ArrayList<>(List.of("Person", "Individual", "Human"));


        testCall(db, "call apoc.dv.catalog.add('" + name + "', " +
                        "{ vrType: 'JDBC', url: '" + url + "', query: 'SELECT * FROM PERSON WHERE NAME = {vrp:pname}', " +
                        " desc: '" + desc + "', labels:['Person','Individual','Human'] })",
                (row) -> {
                    //Node node = (Node) row.get("node");
                    assertEquals(name, row.get("name"));
                    assertEquals("jdbc:derby:derbyDB", row.get("URL"));
                    assertEquals("JDBC", row.get("type"));
                    assertEquals(personTypes, row.get("labels"));
                    assertEquals(desc , row.get("desc"));
                } );

//        testCall(db, "CALL apoc.load.jdbc('jdbc:derby:derbyDB','SELECT * FROM PERSON WHERE NAME = ?',['John'])", //  YIELD row RETURN row
//                (row) -> assertResult(row));


        testCallEmpty(db, "call apoc.dv.query('" + name + "' , { pname: 'Johanna' })", null);

        String personName = "John";
        testCall(db, "call apoc.dv.query('" + name + "' , { pname: '" + personName + "' })",
                (row) -> {
                    Node node = (Node) row.get("node");
                    assertEquals(personName, node.getProperty("name"));
                    assertEquals(personTypes, node.getLabels());
                });


    }


//    @Test
//    public void testSetProperty() throws Exception {
//        testResult(db, "CREATE (n),(m) WITH n,m CALL apoc.create.setProperty([id(n),m],'name','John') YIELD node RETURN node",
//                (result) -> {
//                    Map<String, Object> row = result.next();
//                    assertEquals("John", ((Node) row.get("node")).getProperty("name"));
//                    row = result.next();
//                    assertEquals("John", ((Node) row.get("node")).getProperty("name"));
//                    assertEquals(false, result.hasNext());
//                });
//    }



    private void createPersonTableAndData() throws ClassNotFoundException, SQLException, IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException {
        Class.forName("org.apache.derby.jdbc.EmbeddedDriver").getDeclaredConstructor().newInstance(); // The JDBC specification does not recommend calling newInstance(), but adding a newInstance() call guarantees that Derby will be booted on any JVM. See: http://db.apache.org/derby/docs/10.14/devguide/tdevdvlp20349.html
        if (testName.getMethodName().endsWith(TEST_WITH_AUTHENTICATION)) {
            System.setProperty("derby.connection.requireAuthentication", "true");
            System.setProperty("derby.user.apoc", "Ap0c!#Db");
            conn = DriverManager.getConnection("jdbc:derby:derbyDB;user=apoc;password=Ap0c!#Db;create=true");
        } else {
            conn = DriverManager.getConnection("jdbc:derby:derbyDB;create=true");
        }
        try { conn.createStatement().execute("DROP TABLE PERSON"); } catch (SQLException se) {/*ignore*/}
        conn.createStatement().execute("CREATE TABLE PERSON (NAME varchar(50), SURNAME varchar(50), HIRE_DATE DATE, EFFECTIVE_FROM_DATE TIMESTAMP, TEST_TIME TIME, NULL_DATE DATE)");
        PreparedStatement ps = conn.prepareStatement("INSERT INTO PERSON values(?,null,?,?,?,?)");
        ps.setString(1, "John");
        ps.setDate(2, AbstractJdbcTest.hireDate);
        ps.setTimestamp(3, AbstractJdbcTest.effectiveFromDate);
        ps.setTime(4, AbstractJdbcTest.time);
        ps.setNull(5, Types.DATE);
        int rows = ps.executeUpdate();
        assertEquals(1, rows);
        ResultSet rs = conn.createStatement().executeQuery("SELECT NAME, HIRE_DATE, EFFECTIVE_FROM_DATE, TEST_TIME FROM PERSON");
        assertEquals(true, rs.next());
        assertEquals("John", rs.getString("NAME"));
        Assert.assertEquals(AbstractJdbcTest.hireDate.toLocalDate(), rs.getDate("HIRE_DATE").toLocalDate());
        Assert.assertEquals(AbstractJdbcTest.effectiveFromDate, rs.getTimestamp("EFFECTIVE_FROM_DATE"));
        Assert.assertEquals(AbstractJdbcTest.time, rs.getTime("TEST_TIME"));
        assertEquals(false, rs.next());
        rs.close();
    }
}
