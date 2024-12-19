package apoc.load;

import static apoc.ApocConfig.APOC_IMPORT_FILE_ENABLED;
import static apoc.ApocConfig.APOC_IMPORT_FILE_USE_NEO4J_CONFIG;
import static apoc.ApocConfig.apocConfig;
import static apoc.util.ExtendedTestUtil.assertRelationship;
import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import apoc.util.TestUtil;
import java.util.List;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

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
        final String file =
                ClassLoader.getSystemResource("gexf/single-node.gexf").toString();
        testCall(db, "CALL apoc.load.gexf($file)", Map.of("file", file), (row) -> {
            Map<String, Object> value = (Map) row.get("value");
            String expected =
                    "{_children=[{_children=[{_children=[{_children=[{_children=[{_type=attvalue, for=0, value=http://gephi.org}], _type=attvalues}], _type=node, id=0, label=bar}], _type=nodes}], defaultedgetype=directed, _type=graph}], _type=gexf, version=1.2}";
            assertEquals(expected, value.toString());
        });
    }

    @Test
    public void testImportGexf() {
        final String file = ClassLoader.getSystemResource("gexf/data.gexf").toString();
        TestUtil.testCall(db, "CALL apoc.import.gexf($file, {readLabels:true})", map("file", file), (r) -> {
            assertEquals("gexf", r.get("format"));
            assertEquals(5L, r.get("nodes"));
            assertEquals(8L, r.get("relationships"));
        });

        TestUtil.testCallCount(db, "MATCH (n) RETURN n", 5);

        TestUtil.testResult(db, "MATCH (n:Gephi) RETURN properties(n) as props", r -> {
            ResourceIterator<Map> propsIterator = r.columnAs("props");
            Map props = propsIterator.next();
            assertEquals("http://gephi.org", props.get("0"));
            assertEquals(1.0f, props.get("1"));

            props = propsIterator.next();
            assertEquals("http://test.gephi.org", props.get("0"));
        });

        TestUtil.testResult(db, "MATCH (n:BarabasiLab) RETURN properties(n) as props", r -> {
            ResourceIterator<Map> propsIterator = r.columnAs("props");
            Map props = propsIterator.next();
            assertEquals("http://barabasilab.com", props.get("0"));
            assertEquals(1.0f, props.get("1"));
        });

        Map<String, Object> multiDataTypeNodeProps = Map.of(
                "0",
                "http://gephi.org",
                "1",
                1.0f,
                "room",
                10,
                "price",
                Double.parseDouble("10.02"),
                "projects",
                300L,
                "members",
                new String[] {"Altomare", "Sterpeto", "Lino"},
                "pins",
                new boolean[] {true, false, true, false});

        TestUtil.testResult(db, "MATCH ()-[rel]->() RETURN rel ORDER BY rel.score", r -> {
            final ResourceIterator<Relationship> rels = r.columnAs("rel");

            assertRelationship(
                    rels.next(),
                    "KNOWS",
                    Map.of("score", 1.5f),
                    List.of("Gephi"),
                    multiDataTypeNodeProps,
                    List.of("Webatlas"),
                    Map.of("0", "http://webatlas.fr", "1", 2.0f));

            assertRelationship(
                    rels.next(),
                    "BAZ",
                    Map.of("score", 2.0f, "foo", "bar"),
                    List.of("Gephi"),
                    multiDataTypeNodeProps,
                    List.of("Gephi"),
                    multiDataTypeNodeProps);

            assertRelationship(
                    rels.next(),
                    "HAS_TICKET",
                    Map.of("score", 3f, "ajeje", "brazorf"),
                    List.of("Gephi"),
                    multiDataTypeNodeProps,
                    List.of("RTGI"),
                    Map.of("0", "http://rtgi.fr", "1", 1.0f));

            assertRelationship(
                    rels.next(),
                    "KNOWS",
                    Map.of("score", 4.0f),
                    List.of("Gephi"),
                    multiDataTypeNodeProps,
                    List.of("RTGI"),
                    Map.of("0", "http://rtgi.fr", "1", 1.0f));

            assertRelationship(
                    rels.next(),
                    "KNOWS",
                    Map.of("score", 5.0f),
                    List.of("Webatlas"),
                    Map.of("0", "http://webatlas.fr", "1", 2.0f),
                    List.of("Gephi"),
                    multiDataTypeNodeProps);

            assertRelationship(
                    rels.next(),
                    "KNOWS",
                    Map.of("score", 6.0f),
                    List.of("RTGI"),
                    Map.of("0", "http://rtgi.fr", "1", 1.0f),
                    List.of("Webatlas"),
                    Map.of("0", "http://webatlas.fr", "1", 2.0f));

            assertRelationship(
                    rels.next(),
                    "KNOWS",
                    Map.of("score", 7.0f),
                    List.of("Gephi"),
                    multiDataTypeNodeProps,
                    List.of("Webatlas", "BarabasiLab"),
                    Map.of("0", "http://barabasilab.com", "1", 1.0f, "2", false));

            assertRelationship(
                    rels.next(),
                    "KNOWS",
                    Map.of("score", 8.0f),
                    List.of("Gephi"),
                    Map.of("0", "http://test.gephi.org", "1", 2.0f),
                    List.of("Webatlas", "BarabasiLab"),
                    Map.of("0", "http://barabasilab.com", "1", 1.0f, "2", false));

            assertFalse(rels.hasNext());
        });
    }

    @Test
    public void testImportGexfWithDefaultRelationshipTypeSourceAndTargetConfigs() {
        String defaultRelType = "TEST_DEFAULT";
        final String file =
                ClassLoader.getSystemResource("gexf/single-rel.gexf").toString();

        db.executeTransactionally("CREATE (:Foo {startId: 'start'})");
        db.executeTransactionally("CREATE (:Bar {endId: 'end'})");

        TestUtil.testCall(
                db,
                "CALL apoc.import.gexf($file, {defaultRelationshipType: $defaultRelType, source: $source, target: $target})",
                map(
                        "file",
                        file,
                        "defaultRelType",
                        defaultRelType,
                        "source",
                        map("label", "Foo", "id", "startId"),
                        "target",
                        map("label", "Bar", "id", "endId")),
                (r) -> {
                    assertEquals("gexf", r.get("format"));
                    assertEquals(1L, r.get("relationships"));
                });

        TestUtil.testCall(db, "MATCH ()-[rel]->() RETURN rel", r -> {
            Relationship rel = (Relationship) r.get("rel");
            assertRelationship(
                    rel,
                    defaultRelType,
                    Map.of(),
                    List.of("Foo"),
                    Map.of("startId", "start"),
                    List.of("Bar"),
                    Map.of("endId", "end"));
        });
    }
}
