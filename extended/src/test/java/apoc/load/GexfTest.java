package apoc.load;

import apoc.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.Relationship;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.List;
import java.util.Map;

import static apoc.ApocConfig.APOC_IMPORT_FILE_ENABLED;
import static apoc.ApocConfig.APOC_IMPORT_FILE_USE_NEO4J_CONFIG;
import static apoc.ApocConfig.apocConfig;
import static apoc.util.ExtendedTestUtil.assertRelationship;
import static apoc.util.GexfTestUtil.testImportGexfCommon;
import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class GexfTest {
    
    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();

    @Before
    public void setup() {
        apocConfig().setProperty(APOC_IMPORT_FILE_ENABLED, true);
        apocConfig().setProperty(APOC_IMPORT_FILE_USE_NEO4J_CONFIG, false);
        TestUtil.registerProcedure(db, Gexf.class);
    }

    @After
    public void tearDown() {
        db.shutdown();
    }

    @Test
    public void testLoadGexf() {
        final String file = ClassLoader.getSystemResource("gexf/single-node.gexf").toString();
        testCall(
                db,
                "CALL apoc.load.gexf($file)",
                Map.of("file", file),
                (row) -> {
                    Map<String, Object> value = (Map) row.get("value");
                    String expected = "{_type=gexf, _children=[{_type=graph, defaultedgetype=directed, _children=[{_type=nodes, _children=[{_type=node, _children=[{_type=attvalues, _children=[{_type=attvalue, for=0, value=http://gephi.org}]}], id=0, label=bar}]}]}], version=1.2}";
                    assertEquals(expected, value.toString());
                });
    }

    @Test
    public void testImportGexf() {
        final String file = ClassLoader.getSystemResource("gexf/data.gexf").toString();
        testImportGexfCommon(db, file);
    }

    @Test
    public void testImportGexfWithStoreNodeIds() {
        final String file = ClassLoader.getSystemResource("gexf/single-node.gexf").toString();
        TestUtil.testCall(
                db,
                "CALL apoc.import.gexf($file, {storeNodeIds: true})",
                map("file", file),
                (r) -> {
                    assertEquals("gexf", r.get("format"));
                    assertEquals(1L, r.get("nodes"));
                });

        Map props = TestUtil.singleResultFirstColumn(db, "MATCH (n) RETURN properties(n) AS props");
        assertEquals("http://gephi.org", props.get("0"));
        assertTrue( props.containsKey("id") );
    }
    
    @Test
    public void testImportGexfWithDefaultRelationshipTypeSourceAndTargetConfigs() {
        String defaultRelType = "TEST_DEFAULT";
        final String file = ClassLoader.getSystemResource("gexf/single-rel.gexf").toString();
        
        db.executeTransactionally("CREATE (:Foo {startId: 'start'})");
        db.executeTransactionally("CREATE (:Bar {endId: 'end'})");
        
        TestUtil.testCall(
                db,
                "CALL apoc.import.gexf($file, {defaultRelationshipType: $defaultRelType, source: $source, target: $target})",
                map("file", file, 
                        "defaultRelType", defaultRelType, 
                        "source", map("label", "Foo", "id", "startId"), 
                        "target", map("label", "Bar", "id", "endId")
                ),
                (r) -> {
                    assertEquals("gexf", r.get("format"));
                    assertEquals(1L, r.get("relationships"));
                });

        TestUtil.testCall(db, "MATCH ()-[rel]->() RETURN rel", r -> {
            Relationship rel = (Relationship) r.get("rel");
            assertRelationship(rel, defaultRelType,
                    Map.of(),
                    List.of("Foo"),
                    Map.of("startId", "start"),
                    List.of("Bar"),
                    Map.of("endId", "end")
            );
        });
    }
}
