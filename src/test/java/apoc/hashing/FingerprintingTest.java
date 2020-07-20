package apoc.hashing;

import apoc.coll.Coll;
import apoc.graph.Graphs;
import apoc.util.TestUtil;
import apoc.util.Util;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.EMPTY_LIST;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.neo4j.internal.helpers.collection.MapUtil.map;

public class FingerprintingTest  {

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();

    @Before
    public void setup() {
        TestUtil.registerProcedure(db, Fingerprinting.class, Graphs.class, Coll.class);
    }

    @Test
    public void fingerprintScalars() {

        String hashOfAString = TestUtil.singleResultFirstColumn(db, "return apoc.hashing.fingerprint('some string');");
        assertEquals("5AC749FBEEC93607FC28D666BE85E73A", hashOfAString);

        String hashOfALong = TestUtil.singleResultFirstColumn(db, "return apoc.hashing.fingerprint(123);");
        assertEquals("202CB962AC59075B964B07152D234B70", hashOfALong);

        String hashOfADouble = TestUtil.singleResultFirstColumn( db, "return apoc.hashing.fingerprint(123.456);");
        assertEquals("B316DF1D65EE42FF51A5393DF1F86105", hashOfADouble);

        String hashOfABoolean = TestUtil.singleResultFirstColumn(db, "return apoc.hashing.fingerprint(true);");
        assertEquals("B326B5062B2F0E69046810717534CB09", hashOfABoolean);

        String hashOfNull = TestUtil.singleResultFirstColumn(db, "return apoc.hashing.fingerprint(null);");
        assertEquals("D41D8CD98F00B204E9800998ECF8427E", hashOfNull);
    }

    @Test
    public void fingerprintMap() {
        Map<String, Object> params = map("string", "some string",
                "boolean", true,
                "long", 123l,
                "double", 1234.456d);
        String hashOfAMap = TestUtil.singleResultFirstColumn(db, "return apoc.hashing.fingerprint($map);", singletonMap("map", params));
        assertEquals("040B354004871F76A693DEC2E5DD8F51", hashOfAMap);
    }


    @Test
    public void fingerprintNodeWithArrayProperty() {
        db.executeTransactionally("CREATE (:Person{name:'ABC',emails:['aa@bb.de', 'cc@dd.ee'], integers:[1,2,3], floats:[0.9,1.1]})");
        String value = TestUtil.singleResultFirstColumn(db, "MATCH (n) RETURN apoc.hashing.fingerprint(n) AS hash");
        String value2 = TestUtil.singleResultFirstColumn(db, "MATCH (n) RETURN apoc.hashing.fingerprint(n) AS hash");
        assertEquals(value, value2);
    }

    @Test
    public void fingerprintRelationships() {
        db.executeTransactionally("CREATE (:Person{name:'ABC'})-[:KNOWS{since:12345}]->(:Person{name:'DEF'})");

        String value = TestUtil.singleResultFirstColumn(db, "MATCH ()-[r]->() RETURN apoc.hashing.fingerprint(r) AS hash");
        String value2 = TestUtil.singleResultFirstColumn(db, "MATCH ()-[r]->() RETURN apoc.hashing.fingerprint(r) AS hash");
        assertEquals(value, value2);
    }

    @Test
    public void fingerprintGraph() {
        compareGraph("CREATE (:Person{name:'ABC'})-[:KNOWS{since:12345}]->(:Person{name:'DEF'})", EMPTY_LIST, true);
    }

    @Test
    public void fingerprintGraphShouldFailUponDifferentProperties() {
        compareGraph("CREATE (:Person{name:'ABC', created:timestamp()})", EMPTY_LIST, false);
    }

    @Test
    public void testExcludes() {
        compareGraph("CREATE (:Person{name:'ABC', created:timestamp()})", singletonList("created"), true);
    }

    @Test
    public void testFingerprintingNodeConf() {
        db.executeTransactionally("CREATE (:Person{name:'Andrea',emails:['aa@bb.de', 'cc@dd.ee'], integers:[1,2,3], floats:[0.9,1.1]})");
        String all = TestUtil.singleResultFirstColumn(db, "MATCH (p:Person) return apoc.hashing.fingerprinting(p, $conf) as hash",
                Collections.singletonMap("conf", Collections.emptyMap()));
        String emails = TestUtil.singleResultFirstColumn(db, "MATCH (p:Person) return apoc.hashing.fingerprinting(p, $conf) as hash",
                Collections.singletonMap("conf", Util.map("nodeAllowMap", Util.map("Person", Collections.singletonList("emails")))));
        String emailsByDisallowMap = TestUtil.singleResultFirstColumn(db, "MATCH (p:Person) return apoc.hashing.fingerprinting(p, $conf) as hash",
                Collections.singletonMap("conf", Util.map("nodeDisallowMap", Util.map("Person", Arrays.asList("name", "integers", "floats")))));
        String floats = TestUtil.singleResultFirstColumn(db, "MATCH (p:Person) return apoc.hashing.fingerprinting(p, $conf) as hash",
                Collections.singletonMap("conf", Util.map("nodeDisallowMap", Util.map("Person", Arrays.asList("name", "emails", "integers")))));
        Set<String> hashes = new HashSet<>(Arrays.asList(all, emails, emailsByDisallowMap, floats));
        assertEquals(3, hashes.size()); // 3 because emails = emailsByDisallowMap
    }

    @Test
    public void testFingerprintingRelConf() {
        db.executeTransactionally("CREATE (p1:Person{name:'Andrea'}), (p2:Person{name:'Stefan'}), " +
                "(p1)-[:KNOWS{since: 2014, where: 'Stackoverflow'}]->(p2)");
        String all = TestUtil.singleResultFirstColumn(db, "MATCH (n)-[r]->(m) RETURN apoc.hashing.fingerprinting(r, $conf) as hash",
                Collections.singletonMap("conf", Collections.emptyMap()));
        String since = TestUtil.singleResultFirstColumn(db, "MATCH (n)-[r]->(m) RETURN apoc.hashing.fingerprinting(r, $conf) as hash",
                Collections.singletonMap("conf", Util.map("relAllowMap", Util.map("KNOWS", Collections.singletonList("since")))));
        String where = TestUtil.singleResultFirstColumn(db, "MATCH (n)-[r]->(m) RETURN apoc.hashing.fingerprinting(r, $conf) as hash",
                Collections.singletonMap("conf", Util.map("relDisallowMap", Util.map("KNOWS", Arrays.asList("since")))));
        Set<String> hashes = new HashSet<>(Arrays.asList(all, since, where));
        assertEquals(3, hashes.size());
    }

    @Test
    public void testFingerprintingMapConf() {
        db.executeTransactionally("CREATE (p1:Person{name:'Andrea', surname:'Santurbano'}), (p2:Person{name:'Stefan', surname:'Armbruster'}), " +
                "(pr:Product{sku: 'Nintendo Switch'}), " +
                "(p1)-[:KNOWS{since: 2014}]->(p2), (p2)-[:KNOWS{since: 2018}]->(p1), " +
                "(p1)-[:BOUGHT{when: 2017}]->(pr)");
        String all = TestUtil.singleResultFirstColumn(db, "MATCH p = (n)-[r]->(m) " +
                        "WITH collect(p) AS paths " +
                        "CALL apoc.graph.fromPaths(paths, '', {}) yield graph AS g " +
                        "WITH {nodes: apoc.coll.toSet(g.nodes), rels: apoc.coll.toSet(g.relationships)} AS map " +
                        "RETURN apoc.hashing.fingerprinting(map, $conf) as hash ",
                Collections.singletonMap("conf", Collections.emptyMap()));
    }

    @Test
    public void testFingerprintingWithLazyEvaluation() {
        db.executeTransactionally("CREATE (p1:Person{name:'Andrea', surname:'Santurbano'}), (p2:Person{name:'Stefan', surname:'Armbruster'}), " +
                "(pr:Product{sku: 'Nintendo Switch'}), " +
                "(p1)-[:KNOWS{since: 2014}]->(p2), (p2)-[:KNOWS{since: 2018}]->(p1), " +
                "(p1)-[:BOUGHT{when: 2017}]->(pr)");
        String all = TestUtil.singleResultFirstColumn(db, "MATCH p = (n)-[r]->(m) " +
                        "WITH collect(p) AS paths " +
                        "CALL apoc.graph.fromPaths(paths, '', {}) yield graph AS g " +
                        "WITH {nodes: apoc.coll.toSet(g.nodes), rels: apoc.coll.toSet(g.relationships)} AS map " +
                        "RETURN apoc.hashing.fingerprinting(map, $conf) as hash ",
                Collections.singletonMap("conf", Util.map("nodeAllowMap", Util.map("Person", Collections.singletonList("name")))));
        String filtered = TestUtil.singleResultFirstColumn(db, "MATCH (p:Person) " +
                        "RETURN apoc.hashing.fingerprinting({nodes: collect(p), rels: []}, $conf) as hash ",
                Collections.singletonMap("conf", Util.map("nodeAllowMap", Util.map("Person", Collections.singletonList("name")))));
        assertEquals(all, filtered);
    }

    @Test(expected = QueryExecutionException.class)
    public void testConfigExceptionOnTheSameLabels() {
        db.executeTransactionally("CREATE (p1:Person{name:'Andrea', surname:'Santurbano'}), (p2:Person{name:'Stefan', surname:'Armbruster'}), " +
                "(pr:Product{sku: 'Nintendo Switch'}), " +
                "(p1)-[:KNOWS{since: 2014}]->(p2), (p2)-[:KNOWS{since: 2018}]->(p1), " +
                "(p1)-[:BOUGHT{when: 2017}]->(pr)");
        final Map<String, Object> conf = Util.map("nodeAllowMap", Util.map("Person", Arrays.asList("name")),
                "nodeDisallowMap", Util.map("Person", Arrays.asList("surname")));
        try {
            TestUtil.singleResultFirstColumn(db, "MATCH p = (n)-[r]->(m) " +
                            "WITH collect(p) AS paths " +
                            "CALL apoc.graph.fromPaths(paths, '', {}) yield graph AS g " +
                            "WITH {nodes: apoc.coll.toSet(g.nodes), rels: apoc.coll.toSet(g.relationships)} AS map " +
                            "RETURN apoc.hashing.fingerprinting(map, $conf) as hash ",
                    Collections.singletonMap("conf", conf));
        } catch (Exception e) {
            String expected = "You can't set the same labels for allow and disallow lists for nodes";
            assertEquals(expected, ExceptionUtils.getRootCause(e).getMessage());
            throw e;
        }
    }

    private void compareGraph(String cypher, List<String> excludes, boolean shouldBeEqual) {
        Map<String, Object> params = singletonMap("excludes", excludes);

        db.executeTransactionally(cypher);
        String value = TestUtil.singleResultFirstColumn(db, "return apoc.hashing.fingerprintGraph($excludes) as hash", params);

        db.executeTransactionally("match (n) detach delete n");
        db.executeTransactionally(cypher);
        String value2 = TestUtil.singleResultFirstColumn(db, "return apoc.hashing.fingerprintGraph($excludes) as hash", params);

        if (shouldBeEqual) {
            assertEquals(value, value2);
        } else {
            assertNotEquals(value, value2);
        }

    }
}