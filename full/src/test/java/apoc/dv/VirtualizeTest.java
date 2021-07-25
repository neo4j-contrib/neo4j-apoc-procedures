package apoc.dv;

import apoc.ApocSettings;
import apoc.create.Create;
import apoc.load.LoadCsv;
import apoc.util.TestUtil;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static apoc.util.TestUtil.*;
import static org.junit.Assert.assertEquals;

public class VirtualizeTest {

    public static final Label PERSON = Label.label("Person");

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(ApocSettings.apoc_import_file_enabled, true);

    @Before public void setUp() throws Exception {
        TestUtil.registerProcedure(db, Virtualize.class);
        TestUtil.registerProcedure(db, LoadCsv.class);
        TestUtil.registerProcedure(db, Create.class);
    }

    @Test
    public void testDV() throws Exception {

        String name = "csv_vr";
        String url = getUrlFileName("test.csv").toString();
        String desc = "person's details";


        testCall(db, "call apoc.dv.catalog.add(\"" + name + "\",\n" +
                        "                    { vrType: \"CSV\", url: \"" + url + "\", " +
                        " query: \" row.name = {vrp:name} and  row.age starts with {vrp:age} \", " +
                        " desc: \"" + desc + "\", labels:[\"Person\"], header: true })",
                (row) -> {
                    //Node node = (Node) row.get("node");
                    assertEquals(name, row.get("name"));
                    assertEquals(url, row.get("URL"));
                    assertEquals("CSV", row.get("type"));
                    assertEquals(new ArrayList<>(List.of("Person")), row.get("labels"));
                    assertEquals(desc, row.get("desc"));
                });

        testCall(db, "call apoc.dv.catalog.list() ",
                (result) -> {
                    assertEquals(name, result.get("name"));
                    assertEquals(url, result.get("URL"));
                    assertEquals("CSV", result.get("type"));
                    assertEquals(new ArrayList<>(List.of("Person")), result.get("labels"));
                    assertEquals(desc, result.get("desc"));
                });

        testCall(db, "call apoc.dv.query(\"" + name + "\" , { name: \"Rana\", age: \"1\" })",
                (row) -> {
                    Node node = (Node) row.get("node");
                    assertEquals("Rana", node.getProperty("name"));
                    assertEquals("11", node.getProperty("age"));
                    assertEquals(new ArrayList<>(List.of(Label.label("Person"))), node.getLabels());
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


}
