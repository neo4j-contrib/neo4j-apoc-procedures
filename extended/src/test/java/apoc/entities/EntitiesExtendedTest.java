package apoc.entities;

import apoc.create.Create;
import apoc.util.MapUtil;
import apoc.util.TestUtil;
import apoc.util.Util;
import org.junit.*;
import org.neo4j.graphdb.*;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.time.LocalDate;
import java.util.*;
import java.util.stream.Collectors;

import static apoc.entities.EntitiesExtended.*;
import static apoc.util.TestUtil.*;
import static org.junit.Assert.*;


public class EntitiesExtendedTest {

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();

    @Before
    public void setUp() throws Exception {
        TestUtil.registerProcedure(db, EntitiesExtended.class, Create.class);
        seedGraph();
    }

    @After
    public void teardown() {
        db.shutdown();
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

    // NODE MATCH
    @Test
    public void testMatchNode() {
        testCall(
                db,
               "CALL apoc.node.match(['Actor','Person'],{name:'Giacomino Poretti'}, {bio:'Giacomo Poretti was born on April 26, 1956 in Busto Garolfo....'})",
                (row) -> {
                    Node node = (Node) row.get("node");
                    assertTrue(node.hasLabel(Label.label("Person")));
                    assertTrue(node.hasLabel(Label.label("Actor")));
                    assertEquals("Giacomino Poretti", node.getProperty("name"));
                    assertNotNull(node.getProperty("bio"));
                });
    }

    @Test
    public void testMatchWithoutLabel() {
        testCall(db, "CALL apoc.node.match(null, {name:'Massimo Venier'}) YIELD node RETURN node", (row) -> {
            Node node = (Node) row.get("node");
            assertEquals("Massimo Venier", node.getProperty("name"));
        });
    }

    @Test
    public void testMatchWithoutOnMatchProps() {
        String movieTitle = "Three Men and a Leg";
        testCall(db, "CALL apoc.node.match(['Movie'], $identProps, null) YIELD node RETURN node",
                Util.map("identProps", Util.map("title", movieTitle)),
            (row) -> {
                Node node = (Node) row.get("node");
                assertEquals(movieTitle, node.getProperty("title"));
            });
    }

    @Test
    public void testMatchNodeWithEmptyLabelList() {
        testCall(db, "CALL apoc.node.match([], {name:'Cataldo Baglio'}) YIELD node RETURN node", (row) -> {
            Node node = (Node) row.get("node");
            assertEquals("Cataldo Baglio", node.getProperty("name"));
        });
    }

    @Test
    public void testMatchWithEmptyIdentityPropertiesShouldFail() {
        Set.of("null", "{}").forEach(
                idProps -> failNodeMatchWithMessage(
                        () -> testCall(db,"CALL apoc.node.match(['Person']," + idProps + ", {name:'John'}) YIELD node RETURN node",row -> fail()),
                        IllegalArgumentException.class, INVALID_IDENTIFY_PROPERTY_MESSAGE
                )
        );
    }

    @Test
    public void testMatchNodeWithNullLabelsShouldFail() {
        failNodeMatchWithMessage(
                () -> testCall(db,"CALL apoc.node.match([null], {name:'John'}) YIELD node RETURN node",row -> fail()),
                IllegalArgumentException.class, INVALID_LABELS_MESSAGE
        );
    }

    @Test
    public void testMatchNodeWithMixedLabelsContainingNullShouldFail() {
        failNodeMatchWithMessage(
                () -> testCall(db,"CALL apoc.node.match(['Person', null], {name:'John'}) YIELD node RETURN node",row -> fail()),
                IllegalArgumentException.class, INVALID_LABELS_MESSAGE
        );
    }

    @Test
    public void testMatchNodeWithSingleEmptyLabelShouldFail() {
        failNodeMatchWithMessage(
                () -> testCall(db,"CALL apoc.node.match([''], {name:'John'}) YIELD node RETURN node",row -> fail()),
                IllegalArgumentException.class, INVALID_LABELS_MESSAGE
        );
    }

    @Test
    public void testMatchNodeContainingMixedLabelsContainingEmptyStringShouldFail() {
        failNodeMatchWithMessage(
                () -> testCall(db,"CALL apoc.node.match(['Person', ''], {name:'John'}) YIELD node RETURN node",row -> fail()),
                IllegalArgumentException.class, INVALID_LABELS_MESSAGE
        );
    }

    @Test
    public void testEscapeIdentityPropertiesWithSpecialCharactersShouldWork() {
        MapUtil.map(
                "title", "Three Men and a Leg",
                "y:ear", 1997L,
                "mean-rating", 8L,
                "release date", LocalDate.of(1997, 12, 27)
        ).forEach((key, value) -> {
            Map<String, Object> identProps = MapUtil.map(key, value);
            Map<String, Object> params = MapUtil.map("identProps", identProps);

            testCall(db, "CALL apoc.node.match(['Movie'], $identProps) YIELD node RETURN node", params, (row) -> {
                Node node = (Node) row.get("node");
                assertNotNull(node);
                assertTrue(node.hasProperty(key));
                assertEquals(value, node.getProperty(key));
            });
        });
    }

    @Test
    public void testLabelsWithSpecialCharactersShouldWork() {
        for (String label :
                new String[] {"Label with spaces", ":LabelWithColon", "label-with-dash", "LabelWithUmlautsÄÖÜ"}) {
            db.executeTransactionally(String.format("CREATE (n:`%s` {id:1})",label));
            Map<String, Object> params = MapUtil.map("label", label);
            testCall(
                    db,
                    "CALL apoc.node.match([$label],{id: 1}, {}) YIELD node RETURN node",
                    params,
                    row -> assertTrue(row.get("node") instanceof Node));
            db.executeTransactionally(String.format("MATCH (n:`%s`) DELETE n",label));
        }
    }

    // RELATIONSHIP MATCH

    @Test
    public void testMatchRelationships() {
        testCall(
                db,
                "MATCH (aldo:Person{name:'Cataldo Baglio'}), (movie:Movie) " +
                    "WITH aldo, movie " +
                    "CALL apoc.relationship.match(aldo, 'ACTED_IN', {role:'Aldo'}, movie, {secondaryRoles: ['Ajeje Brazorf', 'Dracula']}) YIELD rel RETURN rel",
                (row) -> {
                    Relationship rel = (Relationship) row.get("rel");
                    assertEquals("ACTED_IN", rel.getType().name());
                    assertEquals("Aldo", rel.getProperty("role"));
                    assertArrayEquals(new String[]{"Ajeje Brazorf", "Dracula"}, (String[]) rel.getProperty("secondaryRoles"));
        });
    }

    @Test
    public void testMatchRelationshipsWithNullOnMatchProps() {
        testCall(
                db,
                "MATCH (giacomino:Person{name:'Giacomino Poretti'}), (movie:Movie) " +
                        "WITH giacomino, movie " +
                        "CALL apoc.relationship.match(giacomino, 'ACTED_IN', {role:'Giacomo'}, movie, null) YIELD rel RETURN rel",
                (row) -> {
                    Relationship rel = (Relationship) row.get("rel");
                    assertEquals("ACTED_IN", rel.getType().name());
                    assertEquals("Giacomo", rel.getProperty("role"));
                });
    }

    @Test
    public void testMatchRelationshipsWithNullIdentProps() {
        testCall(
                db,
                "MATCH (giova:Person{name:'Giovanni Storti'}), (movie:Movie) " +
                        "WITH giova, movie " +
                        "CALL apoc.relationship.match(giova, 'ACTED_IN', null, movie, null) YIELD rel RETURN rel",
                (row) -> {
                    Relationship rel = (Relationship) row.get("rel");
                    assertEquals("ACTED_IN", rel.getType().name());
                    assertEquals("Giovanni", rel.getProperty("role"));
                });
    }

    @Test
    public void testRelationshipTypesWithSpecialCharactersShouldWork() {
        for (String relType : new String[] {"Reltype with space", ":ReltypeWithCOlon", "rel-type-with-dash"}) {
            db.executeTransactionally(String.format("CREATE (:TestStart)-[:`%s`]->(:TestEnd)", relType));
            Map<String, Object> params = MapUtil.map("relType", relType);
            testCall(
                    db,
                    "MATCH (s:TestStart), (e:TestEnd) " +
                        "WITH s,e " +
                        "CALL apoc.relationship.match(s, $relType, null, e, null) YIELD rel RETURN rel",
                    params,
                    row -> {
                        assertTrue(row.get("rel") instanceof Relationship);
                    });
            db.executeTransactionally("MATCH (n) WHERE n:TestStart OR n:TestEnd DETACH DELETE n");
        }
    }

    @Test
    public void testMatchRelWithNullRelTypeShouldFail() {
        failEdgeMatchWithMessage(
                () -> testCall(db,
                        "MATCH (massimo:Director), (movie:Movie) " +
                                "WITH massimo, movie " +
                                "CALL apoc.relationship.match(massimo, null, null, null, movie) YIELD rel RETURN rel",
                        row -> fail()),
                IllegalArgumentException.class, INVALID_REL_TYPE_MESSAGE
        );
    }

    @Test
    public void testMergeWithEmptyRelTypeShouldFail() {
        failEdgeMatchWithMessage(
                () -> testCall(db,
                        "MATCH (massimo:Director), (movie:Movie) " +
                                "WITH massimo, movie " +
                                "CALL apoc.relationship.match(massimo, '', null, null, movie) YIELD rel RETURN rel",
                        row -> fail()),
                IllegalArgumentException.class, INVALID_REL_TYPE_MESSAGE
        );
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

    private void seedGraph(){
        try (Transaction tx = db.beginTx()) {
            tx.execute("""
                    CREATE (giacomo:Person:Actor {name: 'Giacomino Poretti'}),
                        (aldo:Person:Actor {name: 'Cataldo Baglio'}),
                        (giovanni:Person:Actor {name: 'Giovanni Storti'}),
                        (massimo:Person:Director {name: 'Massimo Venier'}),
                        (m:Movie {title: 'Three Men and a Leg', `y:ear`: 1997, `mean-rating`: 8, `release date`: date('1997-12-27')})
                    WITH aldo, giovanni, giacomo, massimo, m
                    CREATE (aldo)-[:ACTED_IN {role: 'Aldo'}]->(m),
                        (giovanni)-[:ACTED_IN {role: 'Giovanni'}]->(m),
                        (giacomo)-[:ACTED_IN {role: 'Giacomo'}]->(m),
                        (massimo)-[:DIRECTED]->(m)
            """);
            tx.commit();
        } catch (RuntimeException e) {
            throw e;
        }
    }

    private void failNodeMatchWithMessage(Runnable lambda, Class<? extends Exception> exceptionType, String message){
        failMatchWithMessage(lambda, exceptionType, message, "apoc.node.match");
    }

    private void failEdgeMatchWithMessage(Runnable lambda, Class<? extends Exception> exceptionType, String message){
        failMatchWithMessage(lambda, exceptionType, message, "apoc.relationship.match");
    }

    private void failMatchWithMessage(Runnable lambda, Class<? extends Exception> exceptionType, String message, String apoc){
        QueryExecutionException queryExecutionException = Assert.assertThrows(QueryExecutionException.class, lambda::run);
        assertError(queryExecutionException, message, exceptionType, apoc);
    }
}
