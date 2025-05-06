package apoc.convert;

import apoc.util.TestUtil;
import apoc.util.Util;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static apoc.util.TestUtil.testCall;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ConvertExtendedTest {
    
    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    @BeforeClass
    public static void setup() {
        TestUtil.registerProcedure(db, ConvertExtended.class);
    }
    
    @Test
    public void testToYamlDockerCompose() {
        String expected = Util.readResourceFile("yml/docker-compose-convert.yml");

        testCall(
                db, """
                        WITH {
                            version:'3.7',
                            services: {
                                neo4j: {
                                    image: 'neo4j:5.18.0-enterprise',
                                    ports: ["7474:7474", "7687:7687"],
                                    environment: {
                                        NEO4J_ACCEPT_LICENSE_AGREEMENT: "yes",
                                        NEO4J_AUTH: "neo4j/password",
                                        NEO4J_dbms_security_procedures_unrestricted: "apoc.*"
                                    },
                                    networks: ['my_net'],
                                    volumes: ['./neo4j/plugins:/plugins']
                                },
                                postgres: {
                                    image: 'postgres:9.6.12', networks: ['my_net']
                                }
                            },
                            networks: {my_net: {driver: 'bridge'}}
                        } AS yaml
                        RETURN apoc.convert.toYaml(yaml, {enable: ['MINIMIZE_QUOTES']}) AS value
                        """,
                (row) -> {
                    assertYamlEquals(expected, row.get("value"));
                });
    }

    @Test
    public void testToYamlWithCustomFeatures() {
        testCall(
                db, """
                        RETURN apoc.convert.toYaml({a:42,b:"foo"},
                            {enable: ['MINIMIZE_QUOTES'], disable: ['WRITE_DOC_START_MARKER']}
                        ) AS value""",
                (row) -> {
                    String expected = "a: 42\n" +
                                      "b: foo\n";
                    assertYamlEquals(expected, row.get("value"));
                });
    }

    @Test
    public void testToYamlList() {
        testCall(
                db, "RETURN apoc.convert.toYaml([1,2,3]) as value", 
                (row) -> {
                    String expected = """
                            ---
                            - 1
                            - 2
                            - 3
                            """;
                    assertEquals(expected, row.get("value"));
                });
    }

    @Test
    public void testToYamlMap() {
        testCall(
                db,
                "RETURN apoc.convert.toYaml({a:42,b:\"foo\",c:[1,2,3]}) as value",
                (row) -> {
                    String expected = """
                            ---
                            a: 42
                            b: "foo"
                            c:
                            - 1
                            - 2
                            - 3
                            """;
                    assertYamlEquals(expected, row.get("value"));
                });
    }

    @Test
    public void testToYamlNode() {
        testCall(db, "CREATE (a:Test {foo: 7}) RETURN apoc.convert.toYaml(a) AS value, elementId(a) AS idNode", (row) -> {
            Object idNode = row.get("idNode");
            String expected = """
                    ---
                    id: "%s"
                    type: "node"
                    labels:
                    - "Test"
                    properties:
                      foo: 7
                      """.formatted(idNode);
            assertYamlEquals(expected, row.get("value"));
        });
    }

    @Test
    public void testToYamlWithNullValues() {
        testCall(db, "RETURN apoc.convert.toYaml({a: null, b: 'myString', c: [1,'2',null]}) as value", (row) -> {
            String expected = """
                    ---
                    a: null
                    b: "myString"
                    c:
                    - 1
                    - "2"
                    - null
                    """;
            assertYamlEquals(expected, row.get("value"));
        });
    }



    @Test
    public void testToYamlNodeWithoutLabel() {
        testCall(db, "CREATE (a {pippo:'pluto'}) RETURN apoc.convert.toYaml(a) AS value, elementId(a) AS id", (row) -> {
            String expected = """
                    ---
                    id: "%s"
                    type: "node"
                    properties:
                      pippo: "pluto"
                      """.formatted(row.get("id"));
            assertYamlEquals(expected, row.get("value"));
        });
    }

    @Test
    public void testToYamlProperties() {
        testCall(
                db,
                "CREATE (a:Test {foo: 7}) RETURN apoc.convert.toYaml(properties(a)) AS value",
                (row) -> {
                    String expected = """
                            ---
                            foo: 7
                            """;
                    assertYamlEquals(expected, row.get("value"));
                });
    }

    @Test
    public void testToYamlMapOfNodes() {
        testCall(
                db,
                "CREATE (a:Test {foo: 7}), (b:Test {bar: 9}) " +
                "RETURN apoc.convert.toYaml({one: a, two: b}) AS value, elementId(a) AS idA, elementId(b) AS idB",
                (row) -> {
                    String expected = """
                            ---
                            one:
                              id: "%s"
                              type: "node"
                              labels:
                              - "Test"
                              properties:
                                foo: 7
                            two:
                              id: "%s"
                              type: "node"
                              labels:
                              - "Test"
                              properties:
                                bar: 9
                            """
                            .formatted( row.get("idA"), row.get("idB") );
                    assertYamlEquals(expected, row.get("value"));
                });
    }


    @Test
    public void testToYamlRel() {
        testCall(
                db,
                "CREATE (start:User {name:'Adam'})-[rel:KNOWS {since: 1993.1, bffSince: duration('P5M1.5D')}]->(end:User {name:'Jim',age:42}) " +
                "   RETURN apoc.convert.toYaml(rel) as value, elementId(start) as idStart, elementId(end) as idEnd, elementId(rel) as idRel",
                (row) -> {
                    String expected = """
                            ---
                            id: "%s"
                            type: "relationship"
                            label: "KNOWS"
                            start:
                              id: "%s"
                              type: "node"
                              labels:
                              - "User"
                              properties:
                                name: "Adam"
                            end:
                              id: "%s"
                              type: "node"
                              labels:
                              - "User"
                              properties:
                                name: "Jim"
                                age: 42
                            properties:
                              bffSince: "P5M1DT12H"
                              since: 1993.1
                              """
                            .formatted( row.get("idRel"), row.get("idStart"), row.get("idEnd") );
                    
                    assertYamlEquals(expected, row.get("value"));
                });
    }

    @Test
    public void testToYamlPath() {
        testCall(
                db,
                "CREATE p=(a:Test {foo: 7})-[r1:TEST]->(b:Baz {a:'b'})<-[r2:TEST_2 {aa:'bb'}]-(c:Bar {one:'www', two:2, three: localdatetime('2020-01-01')}) " +
                "RETURN apoc.convert.toYaml(p) AS value, elementId(a) AS idTest, elementId(b) AS idBaz, elementId(c) AS idBar, elementId(r1) AS idTEST, elementId(r2) AS idTEST_2 ",
                (row) -> {
                    String expected = getExpectedYamlPath().formatted(
                            row.get("idTest"),
                            row.get("idBaz"),
                            row.get("idBar"),
                            row.get("idTEST"),
                            row.get("idTEST_2")
                    );
                    assertYamlEquals(expected, row.get("value"));
                });
    }

    @Test
    public void testToYamlMapOfPath() {
        testCall(db, """
                  CREATE p=(n1:Test {foo: 7})-[r1:TEST]->(n2:Baa:Baz {a:'b'}), q=(n3:Omega {alpha: 'beta'})<-[r2:TEST_2 {aa:'bb'}]-(n4:Bar {boo:'www'})
                  RETURN apoc.convert.toYaml({one: p, two: q}) AS value,
                    elementId(n1) AS idN1, elementId(n2) AS idN2, elementId(n3) AS idN3, elementId(n4) AS idN4, elementId(r1) AS idR1, elementId(r2) AS idR2""",
                (row) -> {
                    String expected = getYamlMapOfPaths()
                            .formatted(
                                    row.get("idN1"), row.get("idN2"), row.get("idN3"), row.get("idN4"), row.get("idR1"), row.get("idR2")
                    );
                    assertYamlEquals(expected, row.get("value"));
                });
    }

    @Test
    public void testFromYamlMapOfPath() {
        String yaml = getYamlMapOfPaths()
                .formatted(
                        "1", "2", "3", "4", "5", "6"
                );

        Map<String, Object> expected = Map.of(
          "one", List.of(
                    Map.of(
                          "id", "1",
                          "type", "node",
                          "properties", Map.of("foo", 7),
                          "labels", List.of("Test")
                    ),
                    Map.of(
                            "start", Map.of(
                                    "id", "1",
                                    "type", "node",
                                    "properties", Map.of("foo", 7),
                                    "labels", List.of("Test")
                            ),
                            "end", Map.of(
                                    "id", "2",
                                    "type", "node",
                                    "properties", Map.of("a", "b"),
                                    "labels", List.of("Baa", "Baz")
                            ),
                        "id", "5",
                        "label", "TEST",

                        "type", "relationship"
                    ),
                    Map.of(
                            "id", "2",
                            "type", "node",
                            "properties", Map.of("a", "b"),
                            "labels", List.of("Baa", "Baz")
                    )
                ),
          "two", List.of(
                        Map.of(
                                "id", "3",
                                "type", "node",
                                "properties", Map.of("alpha", "beta"),
                                "labels", List.of("Omega")
                        ),
                        Map.of(
                                "start", Map.of(
                                        "id", "4",
                                        "type", "node",
                                        "properties", Map.of("boo", "www"),
                                        "labels", List.of("Bar")
                                ),
                                "end", Map.of(
                                        "id", "3",
                                        "type", "node",
                                        "properties", Map.of("alpha", "beta"),
                                        "labels", List.of("Omega")
                                ),
                                "id", "6",
                                "label", "TEST_2",
                                "type", "relationship",
                                "properties", Map.of("aa", "bb")
                        ),
                        Map.of(
                                "id", "4",
                                "type", "node",
                                "properties", Map.of("boo", "www"),
                                "labels", List.of("Bar")
                        )
                )
        );

        testCall(db, """
                  RETURN apoc.convert.fromYaml($yaml, {mapping: {one: "Entity", two: "Entity"} }) AS value
                  """,
                Map.of("yaml", yaml),
                (row) -> assertTrue(expected.equals(row.get("value"))));
    }

    /**
     * Verify the strings ignoring order, as can be change occasionally (e.g. with maps)
     */
    private void  assertYamlEquals(String expected, Object actual) {
        Set<String> expectedSet = Arrays.stream(expected.split("\n")).collect(Collectors.toSet());
        Set<String> actualSet = Arrays.stream(((String) actual).split("\n")).collect(Collectors.toSet());
        
        assertEquals(expectedSet, actualSet);
    }
    
    private static String getYamlMapOfPaths() {
        return """
                ---
                one:
                - id: "%1$s"
                  type: "node"
                  properties:
                    foo: 7
                  labels:
                  - "Test"
                - start:
                    id: "%1$s"
                    type: "node"
                    properties:
                      foo: 7
                    labels:
                    - "Test"
                  end:
                    id: "%2$s"
                    type: "node"
                    properties:
                      a: "b"
                    labels:
                    - "Baa"
                    - "Baz"
                  id: "%5$s"
                  label: "TEST"
                  type: "relationship"
                - id: "%2$s"
                  type: "node"
                  properties:
                    a: "b"
                  labels:
                  - "Baa"
                  - "Baz"
                two:
                - id: "%3$s"
                  type: "node"
                  properties:
                    alpha: "beta"
                  labels:
                  - "Omega"
                - start:
                    id: "%4$s"
                    type: "node"
                    properties:
                      boo: "www"
                    labels:
                    - "Bar"
                  end:
                    id: "%3$s"
                    type: "node"
                    properties:
                      alpha: "beta"
                    labels:
                    - "Omega"
                  id: "%6$s"
                  label: "TEST_2"
                  type: "relationship"
                  properties:
                    aa: "bb"
                - id: "%4$s"
                  type: "node"
                  properties:
                    boo: "www"
                  labels:
                  - "Bar"
                """;
    }

    private static String getExpectedYamlPath() {
        return """
                ---
                - id: "%1$s"
                  type: "node"
                  properties:
                    foo: 7
                  labels:
                  - "Test"
                - start:
                    id: "%1$s"
                    type: "node"
                    properties:
                      foo: 7
                    labels:
                    - "Test"
                  end:
                    id: "%2$s"
                    type: "node"
                    properties:
                      a: "b"
                    labels:
                    - "Baz"
                  id: "%4$s"
                  label: "TEST"
                  type: "relationship"
                - id: "%2$s"
                  type: "node"
                  properties:
                    a: "b"
                  labels:
                  - "Baz"
                - start:
                    id: "%3$s"
                    type: "node"
                    properties:
                      one: "www"
                      three: "2020-01-01T00:00"
                      two: 2
                    labels:
                    - "Bar"
                  end:
                    id: "%2$s"
                    type: "node"
                    properties:
                      a: "b"
                    labels:
                    - "Baz"
                  id: "%5$s"
                  label: "TEST_2"
                  type: "relationship"
                  properties:
                    aa: "bb"
                - id: "%3$s"
                  type: "node"
                  properties:
                    one: "www"
                    three: "2020-01-01T00:00"
                    two: 2
                  labels:
                  - "Bar"
                """;
    }

    @Test
    public void testFromYamlDockerCompose() {
        String fromYaml = Util.readResourceFile("yml/docker-compose-convert.yml");

        Map<String, Object> expected = Map.of(
                "version", 3.7,
                "services", Map.of(
                        "postgres", Map.of(
                                "image", "postgres:9.6.12",
                                "networks", List.of("my_net")
                        ),
                        "neo4j", Map.of(
                                "image", "neo4j:5.18.0-enterprise",
                                "volumes", List.of("./neo4j/plugins:/plugins"),
                                "environment", Map.of(
                                        "NEO4J_dbms_security_procedures_unrestricted", "apoc.*",
                                        "NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes",
                                        "NEO4J_AUTH", "neo4j/password"
                                ),
                                "ports", List.of("7474:7474", "7687:7687"),
                                "networks", List.of("my_net")
                        )
                ),
                "networks", Map.of(
                        "my_net", Map.of("driver", "bridge")
                )
        );

        testCall(
                db, """
                        RETURN apoc.convert.fromYaml($yaml, {enable: ['MINIMIZE_QUOTES']}) AS value
                        """,
                Map.of("yaml", fromYaml),
                (row) -> assertTrue(expected.equals(row.get("value"))));
    }

    @Test
    public void testFromYaml() {
        testCall(
                db, """
                        RETURN apoc.convert.fromYaml("a: 42 
                        b: foo") AS value""",
                (row) -> {
                    Map<String, Object> expected = Map.of("a", 42, "b", "foo");
                    assertEquals(expected, row.get("value"));
                });
    }

    @Test
    public void testFromYamlWithCustomFeaturesAndLongMapping() {
        testCall(
                db, """
                        RETURN apoc.convert.fromYaml("a: 42 
                        b: foo", {mapping: {a: "Long"} }) AS value""",
                (row) -> {
                    Map<String, Object> expected = Map.of("a", 42L, "b", "foo");
                    assertEquals(expected, row.get("value"));
                });
    }

    @Test
    public void testFromYamlWithCustomFeatures() {
        testCall(
                db, """
                        RETURN apoc.convert.fromYaml("a: 42 
                        b: foo", {enable: ['MINIMIZE_QUOTES'], disable: ['WRITE_DOC_START_MARKER']}) AS value""",
                (row) -> {
                    Map<String, Object> expected = Map.of("a", 42, "b", "foo");
                    assertEquals(expected, row.get("value"));
                });
    }

    @Test
    public void testFromYamlList() {
        String fromYaml = """
                            ---
                            - 1
                            - 2
                            - 3
                            """;

        testCall(
                db,
                "RETURN apoc.convert.fromYaml($yaml) as value",
                Map.of("yaml", fromYaml),
                (row) ->  assertEquals(Arrays.asList(1, 2, 3), row.get("value"))
        );
    }

    @Test
    public void testFromYamlListAndLongMapping() {
        String fromYaml = """
                            ---
                            - 1
                            - 2
                            - 3
                            """;

        testCall(
                db,
                "RETURN apoc.convert.fromYaml($yaml, {mapping: {_: \"Long\"} }) as value",
                Map.of("yaml", fromYaml),
                (row) ->  assertEquals(Arrays.asList(1L, 2L, 3L), row.get("value"))
        );
    }

    @Test
    public void testFromYamlMap() {
        String fromYaml = """
                            ---
                            a: 42
                            b: "foo"
                            c:
                            - 1
                            - 2
                            - 3
                            """;

        Map<String, Object> expected = Map.of(
                "a", 42,
                "b", "foo",
                "c", Arrays.asList(1, 2, 3)
        );

        testCall(
                db,
                "RETURN apoc.convert.fromYaml($yaml) as value",
                Map.of("yaml", fromYaml),
                (row) ->  assertEquals(expected, row.get("value"))
        );
    }

    @Test
    public void testFromYamlNode() {
        String fromYaml = """
                    ---
                    id: "3fc16aeb-629f-4181-97d2-a25b22b28b75"
                    type: "node"
                    labels:
                    - "Test"
                    properties:
                      foo: 7
                      """;

        Map<String, Object> expected = Map.of(
                "id", "3fc16aeb-629f-4181-97d2-a25b22b28b75",
                "type", "node",
                "labels", Arrays.asList("Test"),
                "properties", Map.of("foo", 7)
        );

        testCall(
                db,
                "RETURN apoc.convert.fromYaml($yaml) as value",
                Map.of("yaml", fromYaml),
                (row) ->  assertEquals(expected, row.get("value"))
        );
    }

    @Test
    public void testFromYamlWithNullValues() {
        String fromYaml = """
                    ---
                    a: null
                    b: "myString"
                    c:
                    - 1
                    - "2"
                    - null
                    """;

        Map<String, Object> expected = new HashMap<>() {
            {
                put("a", null);
                put("b", "myString");
                put("c", Arrays.asList(1, "2", null));
            }
        };

        testCall(
                db,
                "RETURN apoc.convert.fromYaml($yaml) as value",
                Map.of("yaml", fromYaml),
                (row) ->  assertEquals(expected, row.get("value"))
        );
    }

    @Test
    public void testFromYamlNodeWithoutLabel() {
        String fromYaml = """
                    ---
                    id: "3fc16aeb-629f-4181-97d2-a25b22b28b75"
                    type: "node"
                    properties:
                      pippo: "pluto"
                      """;
        Map<String, Object> expected = Map.of(
                "id", "3fc16aeb-629f-4181-97d2-a25b22b28b75",
                "type", "node",
                "properties", Map.of("pippo", "pluto")
        );
        testCall(
                db,
                "RETURN apoc.convert.fromYaml($yaml) AS value",
                Map.of("yaml", fromYaml),
                (row) -> assertEquals(expected, row.get("value"))
        );
    }

    @Test
    public void testFromYamlProperties() {
        String fromYaml = """
                            ---
                            foo: 7
                            """;

        testCall(db,
                """
                        RETURN apoc.convert.fromYaml($yaml) AS value""",
                Map.of("yaml", fromYaml),
                (row) -> {
                    Map<String, Object> value = (Map<String, Object>) row.get("value");
                    assertEquals(7, value.get("foo"));
                });
    }

    @Test
    public void testFromYamlMapOfNodes() {
        String fromYaml = """
                one:
                  id: "8d3a6b87-39ad-4482-9ce7-5684fe79fc57"
                  type: "node"
                  labels:
                  - "Test"
                  properties:
                    foo: 7
                two:
                  id: "3fc16aeb-629f-4181-97d2-a25b22b28b75"
                  type: "node"
                  labels:
                  - "Test"
                  properties:
                    bar: 9
                """;

        testCall(db,
                """
                        RETURN apoc.convert.fromYaml($yaml) AS value""",
                Map.of("yaml", fromYaml),
                (row) -> {
                    Map<String, Object> value = (Map<String, Object>) row.get("value");
                    assertEquals(2, value.size());

                    Map<String, Object> nodeTest = (Map<String, Object>) value.get("one");
                    assertEquals("node", nodeTest.get("type"));
                });
    }

    @Test
    public void testFromYamlRel() {
        String fromYaml = """
                id: "94996be1-7200-48c2-81e8-479f28bba84d"
                type: "relationship"
                label: "KNOWS"
                start:
                  id: "8d3a6b87-39ad-4482-9ce7-5684fe79fc57"
                  type: "node"
                  labels:
                  - "User"
                  properties:
                    name: "Adam"
                end:
                  id: "3fc16aeb-629f-4181-97d2-a25b22b28b75"
                  type: "node"
                  labels:
                  - "User"
                  properties:
                    name: "Jim"
                    age: 42
                properties:
                  bffSince: "P5M1DT12H"
                  since: 1993.1
                """;
        testCall(db,
                """
                        RETURN apoc.convert.fromYaml($yaml) AS value""",
                Map.of("yaml", fromYaml),
                (row) -> {
                    Map<String, Object> value = (Map<String, Object>) row.get("value");
                    assertEquals("relationship", value.get("type"));
                    assertEquals("KNOWS", value.get("label"));
                });
    }

}
