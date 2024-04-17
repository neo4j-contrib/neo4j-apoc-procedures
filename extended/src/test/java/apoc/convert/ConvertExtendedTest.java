package apoc.convert;

import apoc.util.TestUtil;
import apoc.util.Util;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import static apoc.util.TestUtil.testCall;
import static org.junit.Assert.assertEquals;

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
                  CREATE p=(n1:Test {foo: 7})-[r1:TEST]->(n2:Baa:Baz {a:'b'}), q=(n3:Omega {alpha: 'beta'})<-[r2:TEST_2 {aa:'bb'}]-(n4:Bar {one:'www'})
                  RETURN apoc.convert.toYaml({one: p, two: q}) AS value,
                    elementId(n1) AS idN1, elementId(n2) AS idN2, elementId(n3) AS idN3, elementId(n4) AS idN4, elementId(r1) AS idR1, elementId(r2) AS idR2""",
                (row) -> {
                    String expected = getExpectedYamlMapOfPaths()
                            .formatted(
                                    row.get("idN1"), row.get("idN2"), row.get("idN3"), row.get("idN4"), row.get("idR1"), row.get("idR2")
                    );
                    assertYamlEquals(expected, row.get("value"));
                });
    }

    /**
     * Verify the strings ignoring order, as can be change occasionally (e.g. with maps)
     */
    private void assertYamlEquals(String expected, Object actual) {
        Set<String> expectedSet = Arrays.stream(expected.split("\n")).collect(Collectors.toSet());
        Set<String> actualSet = Arrays.stream(((String) actual).split("\n")).collect(Collectors.toSet());
        
        assertEquals(expectedSet, actualSet);
    }
    
    private static String getExpectedYamlMapOfPaths() {
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
                      one: "www"
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
                    one: "www"
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

}
