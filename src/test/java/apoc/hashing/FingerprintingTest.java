package apoc.hashing;

import apoc.util.TestUtil;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.Result;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.List;
import java.util.Map;

import static java.util.Collections.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.neo4j.internal.helpers.collection.MapUtil.map;

public class FingerprintingTest  {

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();

    @Before
    public void setup() {
        TestUtil.registerProcedure(db);
    }

    @Test
    public void fingerprintScalars() {

        String hashOfAString = cypherSingleResult("return apoc.hashing.fingerprint('some string');");
        assertEquals("5AC749FBEEC93607FC28D666BE85E73A", hashOfAString);

        String hashOfALong = cypherSingleResult("return apoc.hashing.fingerprint(123);");
        assertEquals("202CB962AC59075B964B07152D234B70", hashOfALong);

        String hashOfADouble = cypherSingleResult("return apoc.hashing.fingerprint(123.456);");
        assertEquals("B316DF1D65EE42FF51A5393DF1F86105", hashOfADouble);

        String hashOfABoolean = cypherSingleResult("return apoc.hashing.fingerprint(true);");
        assertEquals("B326B5062B2F0E69046810717534CB09", hashOfABoolean);
    }

    @Test
    public void fingerprintMap() {
        Map<String, Object> params = map("string", "some string",
                "boolean", true,
                "long", 123l,
                "double", 1234.456d);
        String hashOfAMap = cypherSingleResult("return apoc.hashing.fingerprint($map);", singletonMap("map", params));
        assertEquals("040B354004871F76A693DEC2E5DD8F51", hashOfAMap);
    }


    @Test
    public void fingerprintNodeWithArrayProperty() {
        db.execute("CREATE (:Person{name:'ABC',emails:['aa@bb.de', 'cc@dd.ee'], integers:[1,2,3], floats:[0.9,1.1]})");

        String value = cypherSingleResult("MATCH (n) RETURN apoc.hashing.fingerprint(n) AS hash");
        String value2 = cypherSingleResult("MATCH (n) RETURN apoc.hashing.fingerprint(n) AS hash");
        assertEquals(value, value2);
    }

    /**
     * expects a cypher query with one row and one column
     * @param cypherString
     * @return
     */
    private String cypherSingleResult(String cypherString) {
        return cypherSingleResult(cypherString, EMPTY_MAP);
    }

    private String cypherSingleResult(String cypherString, Map<String, Object> params) {
        Result result = db.execute(cypherString, params);
        String columnName = Iterables.single(result.columns());
        return Iterators.single(result.columnAs(columnName));
    }

    @Test
    public void fingerprintRelationships() {
        db.execute("CREATE (:Person{name:'ABC'})-[:KNOWS{since:12345}]->(:Person{name:'DEF'})");

        String value = cypherSingleResult("MATCH ()-[r]->() RETURN apoc.hashing.fingerprint(r) AS hash");
        String value2 = cypherSingleResult("MATCH ()-[r]->() RETURN apoc.hashing.fingerprint(r) AS hash");
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

    private void compareGraph(String cypher, List<String> excludes, boolean shouldBeEqual) {
        Map<String, Object> params = singletonMap("excludes", excludes);

        db.execute(cypher);
        String value = Iterators.single(db.execute("return apoc.hashing.fingerprintGraph($excludes) as hash", params).columnAs("hash"));

        db.execute("match (n) detach delete n");
        db.execute(cypher);
        String value2 = Iterators.single(db.execute("return apoc.hashing.fingerprintGraph($excludes) as hash", params).columnAs("hash"));

        if (shouldBeEqual) {
            assertEquals(value, value2);
        } else {
            assertNotEquals(value, value2);
        }

    }
}
