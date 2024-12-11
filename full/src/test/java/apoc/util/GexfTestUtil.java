package apoc.util;

import static apoc.util.ExtendedTestUtil.assertRelationship;
import static apoc.util.MapUtil.map;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.List;
import java.util.Map;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.test.rule.DbmsRule;

public class GexfTestUtil {
    public static void testImportGexfCommon(DbmsRule db, String file) {
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
                    Map.of(),
                    List.of("Gephi"),
                    multiDataTypeNodeProps,
                    List.of("RTGI"),
                    Map.of("0", "http://rtgi.fr", "1", 1.0f));

            assertRelationship(
                    rels.next(),
                    "KNOWS",
                    Map.of(),
                    List.of("Webatlas"),
                    Map.of("0", "http://webatlas.fr", "1", 2.0f),
                    List.of("Gephi"),
                    multiDataTypeNodeProps);

            assertRelationship(
                    rels.next(),
                    "KNOWS",
                    Map.of(),
                    List.of("RTGI"),
                    Map.of("0", "http://rtgi.fr", "1", 1.0f),
                    List.of("Webatlas"),
                    Map.of("0", "http://webatlas.fr", "1", 2.0f));

            assertRelationship(
                    rels.next(),
                    "KNOWS",
                    Map.of(),
                    List.of("Gephi"),
                    multiDataTypeNodeProps,
                    List.of("Webatlas", "BarabasiLab"),
                    Map.of("0", "http://barabasilab.com", "1", 1.0f, "2", false));

            assertRelationship(
                    rels.next(),
                    "KNOWS",
                    Map.of(),
                    List.of("Gephi"),
                    Map.of("0", "http://test.gephi.org", "1", 2.0f),
                    List.of("Webatlas", "BarabasiLab"),
                    Map.of("0", "http://barabasilab.com", "1", 1.0f, "2", false));

            assertFalse(rels.hasNext());
        });
    }
}
