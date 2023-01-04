package apoc.export.graphml;

import apoc.graph.Graphs;
import apoc.util.TestUtil;
import org.junit.rules.TestName;
import org.neo4j.graphdb.GraphDatabaseService;
import org.xmlunit.builder.DiffBuilder;
import org.xmlunit.diff.DefaultNodeMatcher;
import org.xmlunit.diff.Diff;
import org.xmlunit.diff.ElementSelector;
import org.xmlunit.util.Nodes;

import javax.xml.namespace.QName;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static apoc.ApocConfig.APOC_EXPORT_FILE_ENABLED;
import static apoc.ApocConfig.APOC_IMPORT_FILE_ENABLED;
import static apoc.ApocConfig.APOC_IMPORT_FILE_USE_NEO4J_CONFIG;
import static apoc.ApocConfig.apocConfig;
import static org.junit.Assert.assertFalse;
import static org.xmlunit.diff.ElementSelectors.byName;

public class ExportGraphMLTestUtil {
    private static final String KEY_TYPES_EMPTY = "<key id=\"name\" for=\"node\" attr.name=\"name\" attr.type=\"string\"/>%n" +
            "<key id=\"limit\" for=\"node\" attr.name=\"limit\" attr.type=\"long\"/>%n" +
            "<key id=\"labels\" for=\"node\" attr.name=\"labels\" attr.type=\"string\"/>%n";
    private static final String GRAPH = "<graph id=\"G\" edgedefault=\"directed\">%n";
    private static final String HEADER = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>%n" +
            "<graphml xmlns=\"http://graphml.graphdrawing.org/xmlns\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:schemaLocation=\"http://graphml.graphdrawing.org/xmlns http://graphml.graphdrawing.org/xmlns/1.0/graphml.xsd\">%n";
    private static final String KEY_TYPES_FALSE = "<key id=\"born\" for=\"node\" attr.name=\"born\"/>%n" +
            "<key id=\"values\" for=\"node\" attr.name=\"values\"/>%n" +
            "<key id=\"name\" for=\"node\" attr.name=\"name\"/>%n" +
            "<key id=\"labels\" for=\"node\" attr.name=\"labels\"/>%n"+
            "<key id=\"place\" for=\"node\" attr.name=\"place\"/>%n" +
            "<key id=\"age\" for=\"node\" attr.name=\"age\"/>%n" +
            "<key id=\"label\" for=\"edge\" attr.name=\"label\"/>%n";
    private static final String KEY_TYPES_FALSE_TINKER = "<key id=\"born\" for=\"node\" attr.name=\"born\"/>%n" +
            "<key id=\"name\" for=\"node\" attr.name=\"name\"/>%n" +
            "<key id=\"labelV\" for=\"node\" attr.name=\"labelV\"/>%n"+
            "<key id=\"place\" for=\"node\" attr.name=\"place\"/>%n" +
            "<key id=\"age\" for=\"node\" attr.name=\"age\"/>%n" +
            "<key id=\"values\" for=\"node\" attr.name=\"values\"/>" +
            "<key id=\"labelE\" for=\"edge\" attr.name=\"labelE\"/>%n";
    private static final String KEY_TYPES_DATA = "<key id=\"name\" for=\"node\" attr.name=\"name\"/>\n" +
            "<key id=\"labels\" for=\"node\" attr.name=\"labels\"/>";
    private static final String KEY_TYPES = "<key id=\"born\" for=\"node\" attr.name=\"born\" attr.type=\"string\"/>%n" +
            "<key id=\"values\" for=\"node\" attr.name=\"values\" attr.type=\"string\" attr.list=\"long\"/>%n" +
            "<key id=\"name\" for=\"node\" attr.name=\"name\" attr.type=\"string\"/>%n" +
            "<key id=\"labels\" for=\"node\" attr.name=\"labels\" attr.type=\"string\"/>%n"+
            "<key id=\"place\" for=\"node\" attr.name=\"place\" attr.type=\"string\"/>%n" +
            "<key id=\"age\" for=\"node\" attr.name=\"age\" attr.type=\"long\"/>%n" +
            "<key id=\"label\" for=\"edge\" attr.name=\"label\" attr.type=\"string\"/>%n";
    private static final String KEY_TYPES_PATH = "<key id=\"born\" for=\"node\" attr.name=\"born\" attr.type=\"string\"/>%n" +
            "<key id=\"name\" for=\"node\" attr.name=\"name\" attr.type=\"string\"/>%n" +
            "<key id=\"labels\" for=\"node\" attr.name=\"labels\" attr.type=\"string\"/>%n"+
            "<key id=\"place\" for=\"node\" attr.name=\"place\" attr.type=\"string\"/>%n" +
            "<key id=\"TYPE\" for=\"node\" attr.name=\"TYPE\" attr.type=\"string\"/>%n" +
            "<key id=\"age\" for=\"node\" attr.name=\"age\" attr.type=\"long\"/>%n" +
            "<key id=\"label\" for=\"edge\" attr.name=\"label\" attr.type=\"string\"/>%n" +
            "<key id=\"TYPE\" for=\"edge\" attr.name=\"TYPE\" attr.type=\"string\"/>%n";
    private static final String KEY_TYPES_PATH_TINKERPOP = "<key id=\"born\" for=\"node\" attr.name=\"born\" attr.type=\"string\"/>%n" +
            "<key id=\"name\" for=\"node\" attr.name=\"name\" attr.type=\"string\"/>%n" +
            "<key id=\"labelV\" for=\"node\" attr.name=\"labelV\" attr.type=\"string\"/>%n"+
            "<key id=\"place\" for=\"node\" attr.name=\"place\" attr.type=\"string\"/>%n" +
            "<key id=\"age\" for=\"node\" attr.name=\"age\" attr.type=\"long\"/>%n" +
            "<key id=\"labelE\" for=\"edge\" attr.name=\"labelE\" attr.type=\"string\"/>%n";
    private static final String KEY_TYPES_CAMEL_CASE = "<key id=\"firstName\" for=\"node\" attr.name=\"firstName\" attr.type=\"string\"/>%n" +
            "<key id=\"ageNow\" for=\"node\" attr.name=\"ageNow\" attr.type=\"long\"/>%n" +
            "<key id=\"name\" for=\"node\" attr.name=\"name\" attr.type=\"string\"/>%n" +
            "<key id=\"labels\" for=\"node\" attr.name=\"labels\" attr.type=\"string\"/>%n" +
            "<key id=\"TYPE\" for=\"node\" attr.name=\"TYPE\" attr.type=\"string\"/>%n" +
            "<key id=\"label\" for=\"edge\" attr.name=\"label\" attr.type=\"string\"/>%n" +
            "<key id=\"TYPE\" for=\"edge\" attr.name=\"TYPE\" attr.type=\"string\"/>%n";
    private static final String KEY_TYPES_NO_DATA_KEY = "<key id=\"Node.Path\" for=\"node\" attr.name=\"Path\" attr.type=\"string\"/>\n" +
            "<key id=\"Edge.Path\" for=\"edge\" attr.name=\"Path\" attr.type=\"string\"/>";
    private static final String DATA = "<node id=\"n0\" labels=\":Foo:Foo0:Foo2\"><data key=\"labels\">:Foo:Foo0:Foo2</data><data key=\"place\">{\"crs\":\"wgs-84-3d\",\"latitude\":12.78,\"longitude\":56.7,\"height\":100.0}</data><data key=\"name\">foo</data><data key=\"born\">2018-10-10</data></node>%n" +
            "<node id=\"n1\" labels=\":Bar\"><data key=\"labels\">:Bar</data><data key=\"age\">42</data><data key=\"name\">bar</data><data key=\"place\">{\"crs\":\"wgs-84\",\"latitude\":12.78,\"longitude\":56.7,\"height\":null}</data></node>%n" +
            "<node id=\"n2\" labels=\":Bar\"><data key=\"labels\">:Bar</data><data key=\"age\">12</data><data key=\"values\">[1,2,3]</data></node>%n" +
            "<edge id=\"e0\" source=\"n0\" target=\"n1\" label=\"KNOWS\"><data key=\"label\">KNOWS</data></edge>%n";
    private static final String DATA_TINKER = "<node id=\"n0\"><data key=\"labelV\">Foo:Foo0:Foo2</data><data key=\"place\">{\"crs\":\"wgs-84-3d\",\"latitude\":12.78,\"longitude\":56.7,\"height\":100.0}</data><data key=\"name\">foo</data><data key=\"born\">2018-10-10</data></node>%n" +
            "<node id=\"n1\"><data key=\"labelV\">Bar</data><data key=\"age\">42</data><data key=\"name\">bar</data><data key=\"place\">{\"crs\":\"wgs-84\",\"latitude\":12.78,\"longitude\":56.7,\"height\":null}</data></node>%n" +
            "<node id=\"n2\"><data key=\"labelV\">Bar</data><data key=\"age\">12</data><data key=\"values\">[1,2,3]</data></node>%n" +
            "<edge id=\"e0\" source=\"n0\" target=\"n1\"><data key=\"labelE\">KNOWS</data></edge>%n";
    private static final String DATA_CAMEL_CASE =
            "<node id=\"n0\" labels=\":Foo:Foo0:Foo2\"><data key=\"TYPE\">:Foo:Foo0:Foo2</data><data key=\"label\">foo</data><data key=\"firstName\">foo</data></node>%n" +
                    "<node id=\"n1\" labels=\":Bar\"><data key=\"TYPE\">:Bar</data><data key=\"label\">bar</data><data key=\"name\">bar</data><data key=\"ageNow\">42</data></node>%n" +
                    "<edge id=\"e0\" source=\"n0\" target=\"n1\" label=\"KNOWS\"><data key=\"label\">KNOWS</data><data key=\"TYPE\">KNOWS</data></edge>%n";

    private static final String DATA_NODE_EDGE = "<node id=\"n0\"> <data key=\"labels\">:FOO</data><data key=\"name\">foo</data> </node>%n" +
            "<node id=\"n1\"> <data key=\"labels\">:BAR</data><data key=\"name\">bar</data> <data key=\"kids\">[a,b,c]</data> </node>%n" +
            "<edge id=\"e0\" source=\"n0\" target=\"n1\"> <data key=\"label\">:EDGE_LABEL</data> <data key=\"name\">foo</data> </edge>%n" +
            "<edge id=\"e1\" source=\"n1\" target=\"n0\"><data key=\"label\">TEST</data> </edge>%n" +
            "<node id=\"n3\"> <data key=\"labels\">:QWERTY</data><data key=\"name\">qwerty</data> </node>%n" +
            "<edge id=\"e2\" source=\"n1\" target=\"n3\"> <data key=\"label\">KNOWS</data> </edge>%n";

    private static final String DATA_WITHOUT_CHAR_DATA_KEYS = "<node id=\"n0\" labels=\":Foo:Foo0:Foo2\"><data key=\"labels\">:Foo:Foo0:Foo2</data><data key=\"place\">{\"crs\":\"wgs-84-3d\",\"latitude\":12.78,\"longitude\":56.7,\"height\":100.0}</data><data key=\"name\">foo</data><data key=\"born\">2018-10-10</data></node>%n" +
            "<node id=\"n1\" labels=\":Bar\"><data key=\"labels\">:Bar</data><data key=\"age\">42</data><data key=\"name\">bar</data><data key=\"place\">{\"crs\":\"wgs-84\",\"latitude\":12.78,\"longitude\":56.7,\"height\":null}</data></node>%n" +
            "<node id=\"n2\" labels=\":Bar\"><data key=\"labels\">:Bar</data><data key=\"age\">12</data><data key=\"values\">[1,2,3]</data></node>%n" +
            "<edge id=\"e0\" source=\"n0\" target=\"n1\"><data key=\"d10\"/></edge>\n";

    private static final String DATA_NO_DATA_KEY = "<node id=\"A\" labels=\":Unit\"><data key=\"Path\">C:\\bright\\itecembed\\obj\\ada\\a3_status.ads</data></node>\n" +
            "<node id=\"B\" labels=\":Unit\"><data key=\"Path\">C:\\bright\\itecembed\\obj\\ada\\b3_status.ads</data></node>\n" +
            "<edge source=\"A\" target=\"B\"><!-- <data key=\"Path\">C:\\bright\\itecembed\\obj\\ada\\b3_status.ads</data> --></edge>";

    private static final String FOOTER = "</graph>%n" +
            "</graphml>";

    private static final String DATA_PATH = "<node id=\"n0\" labels=\":Foo:Foo0:Foo2\"><data key=\"TYPE\">:Foo:Foo0:Foo2</data><data key=\"label\">foo</data><data key=\"place\">{\"crs\":\"wgs-84-3d\",\"latitude\":12.78,\"longitude\":56.7,\"height\":100.0}</data><data key=\"name\">foo</data><data key=\"born\">2018-10-10</data></node>%n" +
            "<node id=\"n1\" labels=\":Bar\"><data key=\"TYPE\">:Bar</data><data key=\"label\">bar</data><data key=\"age\">42</data><data key=\"name\">bar</data><data key=\"place\">{\"crs\":\"wgs-84\",\"latitude\":12.78,\"longitude\":56.7,\"height\":null}</data></node>%n" +
            "<edge id=\"e0\" source=\"n0\" target=\"n1\" label=\"KNOWS\"><data key=\"label\">KNOWS</data><data key=\"TYPE\">KNOWS</data></edge>%n";

    private static final String DATA_PATH_CAPTION = "<node id=\"n0\" labels=\":Foo:Foo0:Foo2\"><data key=\"TYPE\">:Foo:Foo0:Foo2</data><data key=\"label\">foo</data><data key=\"place\">{\"crs\":\"wgs-84-3d\",\"latitude\":12.78,\"longitude\":56.7,\"height\":100.0}</data><data key=\"name\">foo</data><data key=\"born\">2018-10-10</data></node>%n" +
            "<node id=\"n1\" labels=\":Bar\"><data key=\"TYPE\">:Bar</data><data key=\"label\">bar</data><data key=\"age\">42</data><data key=\"name\">bar</data><data key=\"place\">{\"crs\":\"wgs-84\",\"latitude\":12.78,\"longitude\":56.7,\"height\":null}</data></node>%n" +
            "<edge id=\"e0\" source=\"n0\" target=\"n1\" label=\"KNOWS\"><data key=\"label\">KNOWS</data><data key=\"TYPE\">KNOWS</data></edge>%n";

    private static final String DATA_PATH_CAPTION_TINKER = "<node id=\"n0\"><data key=\"labelV\">Foo:Foo0:Foo2</data><data key=\"place\">{\"crs\":\"wgs-84-3d\",\"latitude\":12.78,\"longitude\":56.7,\"height\":100.0}</data><data key=\"name\">foo</data><data key=\"born\">2018-10-10</data></node>%n" +
            "<node id=\"n1\"><data key=\"labelV\">Bar</data><data key=\"age\">42</data><data key=\"name\">bar</data><data key=\"place\">{\"crs\":\"wgs-84\",\"latitude\":12.78,\"longitude\":56.7,\"height\":null}</data></node>%n" +
            "<edge id=\"e0\" source=\"n0\" target=\"n1\"><data key=\"labelE\">KNOWS</data></edge>%n";

    private static final String DATA_PATH_CAPTION_DEFAULT = "<node id=\"n0\" labels=\":Foo:Foo0:Foo2\"><data key=\"TYPE\">:Foo:Foo0:Foo2</data><data key=\"label\">point({x: 56.7, y: 12.78, z: 100.0, crs: 'wgs-84-3d'})</data><data key=\"place\">{\"crs\":\"wgs-84-3d\",\"latitude\":12.78,\"longitude\":56.7,\"height\":100.0}</data><data key=\"name\">foo</data><data key=\"born\">2018-10-10</data></node>%n" +
            "<node id=\"n1\" labels=\":Bar\"><data key=\"TYPE\">:Bar</data><data key=\"label\">point({x: 56.7, y: 12.78, crs: 'wgs-84'})</data><data key=\"age\">42</data><data key=\"name\">bar</data><data key=\"place\">{\"crs\":\"wgs-84\",\"latitude\":12.78,\"longitude\":56.7,\"height\":null}</data></node>%n" +
            "<edge id=\"e0\" source=\"n0\" target=\"n1\" label=\"KNOWS\"><data key=\"label\">KNOWS</data><data key=\"TYPE\">KNOWS</data></edge>%n";

    private static final String DATA_DATA = "<node id=\"n3\" labels=\":Person\"><data key=\"labels\">:Person</data><data key=\"name\">Foo</data></node>\n" +
            "<node id=\"n5\" labels=\":Person\"><data key=\"labels\">:Person</data><data key=\"name\">Foo0</data></node>\n";

    public static final String EXPECTED_TYPES_PATH = String.format(HEADER + KEY_TYPES_PATH + GRAPH + DATA_PATH + FOOTER);
    public static final String EXPECTED_TYPES_PATH_CAPTION = String.format(HEADER + KEY_TYPES_PATH + GRAPH + DATA_PATH_CAPTION + FOOTER);
    public static final String EXPECTED_TYPES_PATH_CAPTION_TINKER = String.format(HEADER + KEY_TYPES_PATH_TINKERPOP + GRAPH + DATA_PATH_CAPTION_TINKER + FOOTER);
    public static final String EXPECTED_TYPES_PATH_WRONG_CAPTION = String.format(HEADER + KEY_TYPES_PATH + GRAPH + DATA_PATH_CAPTION_DEFAULT + FOOTER);
    public static final String EXPECTED_TYPES = String.format(HEADER + KEY_TYPES + GRAPH + DATA + FOOTER);
    public static final String EXPECTED_TYPES_WITHOUT_CHAR_DATA_KEYS = String.format(HEADER + KEY_TYPES  + GRAPH + DATA_WITHOUT_CHAR_DATA_KEYS + FOOTER);
    public static final String EXPECTED_FALSE = String.format(HEADER + KEY_TYPES_FALSE + GRAPH + DATA + FOOTER);
    public static final String EXPECTED_TINKER = String.format(HEADER + KEY_TYPES_FALSE_TINKER + GRAPH + DATA_TINKER + FOOTER);
    public static final String EXPECTED_DATA = String.format(HEADER + KEY_TYPES_DATA + GRAPH + DATA_DATA + FOOTER);
    public static final String EXPECTED_READ_NODE_EDGE = String.format(HEADER + GRAPH + DATA_NODE_EDGE + FOOTER);
    public static final String EXPECTED_TYPES_PATH_CAMEL_CASE = String.format(HEADER + KEY_TYPES_CAMEL_CASE + GRAPH + DATA_CAMEL_CASE + FOOTER);
    public static final String DATA_EMPTY = "<node id=\"n0\" labels=\":Test\"><data key=\"labels\">:Test</data><data key=\"name\"></data><data key=\"limit\">3</data></node>%n";
    public static final String EXPECTED_TYPES_EMPTY = String.format(HEADER + KEY_TYPES_EMPTY + GRAPH + DATA_EMPTY + FOOTER);
    public static final String EXPECTED_TYPES_NO_DATA_KEY = String.format(HEADER + KEY_TYPES_NO_DATA_KEY + GRAPH + DATA_NO_DATA_KEY + FOOTER);

    
    public static void assertXMLEquals(Object output, String xmlString) {
        List<String> attrsWithNodeIds = Arrays.asList("id", "source", "target");
        
        Diff myDiff = DiffBuilder.compare(xmlString)
                .withTest(output)
                .checkForSimilar()
                .ignoreWhitespace()
                .withAttributeFilter(attr -> !attrsWithNodeIds.contains(attr.getLocalName())) // ignore id properties
                // similar to ElementSelectors.byNameAndAllAttributes, but ignore blacklistes attributes
                .withNodeMatcher(new DefaultNodeMatcher((ElementSelector) (controlElement, testElement) -> {
                    if (!byName.canBeCompared(controlElement, testElement)) {
                        return false;
                    }
                    Map<QName, String> cAttrs = Nodes.getAttributes(controlElement);
                    Map<QName, String> tAttrs = Nodes.getAttributes(testElement);
                    if (cAttrs.size() != tAttrs.size()) {
                        return false;
                    }
                    for (Map.Entry<QName, String> e: cAttrs.entrySet()) {
                        if ((!attrsWithNodeIds.contains(e.getKey().getLocalPart()))
                                && (!e.getValue().equals(tAttrs.get(e.getKey())))) {
                            return false;
                        }
                    }
                    return true;
                }))
                .build();

        assertFalse(myDiff.toString(), myDiff.hasDifferences());
    }


    public static void setUpGraphMl(GraphDatabaseService db, TestName testName) {
        TestUtil.registerProcedure(db, ExportGraphML.class, Graphs.class);
        
        apocConfig().setProperty(APOC_EXPORT_FILE_ENABLED, 
                Boolean.toString(!testName.getMethodName().endsWith("WithNoExportConfig")));
        apocConfig().setProperty(APOC_IMPORT_FILE_ENABLED, 
                Boolean.toString(!testName.getMethodName().endsWith("WithNoImportConfig")));
        apocConfig().setProperty(APOC_IMPORT_FILE_USE_NEO4J_CONFIG, false);

        db.executeTransactionally("CREATE (f:Foo:Foo2:Foo0 {name:'foo', born:Date('2018-10-10'), place:point({ longitude: 56.7, latitude: 12.78, height: 100 })})-[:KNOWS]->(b:Bar {name:'bar',age:42, place:point({ longitude: 56.7, latitude: 12.78})}),(c:Bar {age:12,values:[1,2,3]})");
    }
}
