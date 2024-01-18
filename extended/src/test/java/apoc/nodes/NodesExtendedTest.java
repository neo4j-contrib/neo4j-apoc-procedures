package apoc.nodes;

import apoc.create.Create;
import apoc.util.TestUtil;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;


public class NodesExtendedTest {

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();

    @Before
    public void setUp() throws Exception {
        TestUtil.registerProcedure(db, NodesExtended.class, Create.class);
    }

    @Test
    public void rebind() {
        TestUtil.testCall(db, "CREATE (a:Foo)-[r1:MY_REL]->(b:Bar)-[r2:ANOTHER_REL]->(c:Baz) WITH a,b,c,r1,r2 \n" +
                        "WITH apoc.any.rebind({first: a, second: b, third: c, rels: [r1, r2]}) as rebind RETURN rebind, valueType(rebind) as value",
                (row) -> {
                    final Map<String, Object> rebind = (Map<String, Object>) row.get("rebind");
                    final List<Relationship> rels = (List<Relationship>) rebind.get("rels");
                    final Relationship firstRel = rels.get(0);
                    final Relationship secondRel = rels.get(1);
                    assertEquals(firstRel.getStartNode(), rebind.get("first"));
                    assertEquals(firstRel.getEndNode(), rebind.get("second"));
                    assertEquals(secondRel.getStartNode(), rebind.get("second"));
                    assertEquals(secondRel.getEndNode(), rebind.get("third"));
                });

        TestUtil.testCall(db, "CREATE p1=(a:Foo)-[r1:MY_REL]->(b:Bar), p2=(:Bar)-[r2:ANOTHER_REL]->(c:Baz) \n" +
                        "RETURN apoc.any.rebind([p1, p2]) as rebind",
                (row) -> {
                    final List<Path> rebindList = (List<Path>) row.get("rebind");
                    assertEquals(2, rebindList.size());
                    final Path firstPath = rebindList.get(0);
                    assertFooBarPath(firstPath);
                    final Path secondPath = rebindList.get(1);
                    assertPath(secondPath, List.of("Bar", "Baz"), List.of("ANOTHER_REL"));
                });

        // check via `valueType()` that, even if the return type is Object, 
        // the output of a rebound Path is also a Path (i.e.: `PATH NOT NULL`)
        TestUtil.testCall(db, """
                        CREATE path=(a:Foo)-[r1:MY_REL]->(b:Bar)\s
                        WITH apoc.any.rebind(path) AS rebind
                        RETURN rebind, valueType(rebind) as valueType""",
                (row) -> {
                    final String valueType = (String) row.get("valueType");
                    assertEquals("PATH NOT NULL", valueType);
                    
                    final Path pathRebind = (Path) row.get("rebind");
                    assertFooBarPath(pathRebind);

                });
    }

    private void assertFooBarPath(Path pathRebind) {
        assertPath(pathRebind, List.of("Foo", "Bar"), List.of("MY_REL"));
    }

    private void assertPath(Path rebind, List<String> labels, List<String> relTypes) {
        final List<String> actualLabels = Iterables.stream(rebind.nodes())
                .map(i -> i.getLabels().iterator().next())
                .map(Label::name).collect(Collectors.toList());
        assertEquals(labels, actualLabels);
        final List<String> actualRelTypes = Iterables.stream(rebind.relationships()).map(Relationship::getType)
                .map(RelationshipType::name).collect(Collectors.toList());
        assertEquals(relTypes, actualRelTypes);
    }
}
