package apoc.hashing;

import apoc.util.TestUtil;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
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
        TestUtil.registerProcedure(db, Fingerprinting.class);
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
