package apoc.hashing;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.harness.junit.Neo4jRule;
import org.neo4j.helpers.collection.Iterators;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class FingerprintingTest {

    @Rule
    public Neo4jRule neo4j = new Neo4jRule()
            .withFunction(Fingerprinting.class);

    @Test
    public void fingerprintNodeWithArrayProperty() {
        GraphDatabaseService db = neo4j.getGraphDatabaseService();
        db.execute("CREATE (:Person{name:'ABC',emails:['aa@bb.de', 'cc@dd.ee'], integers:[1,2,3], floats:[0.9,1.1]})");

        String value = Iterators.single(db.execute("MATCH (n) RETURN apoc.hashing.fingerprintNodeOrRel(n) AS hash").columnAs("hash"));
        String value2 = Iterators.single(db.execute("MATCH (n) RETURN apoc.hashing.fingerprintNodeOrRel(n) AS hash").columnAs("hash"));
        assertEquals(value, value2);
    }

    @Test
    public void fingerprintRelationships() {
        GraphDatabaseService db = neo4j.getGraphDatabaseService();
        db.execute("CREATE (:Person{name:'ABC'})-[:KNOWS{since:12345}]->(:Person{name:'DEF'})");

        String value = Iterators.single(db.execute("MATCH ()-[r]->() RETURN apoc.hashing.fingerprintNodeOrRel(r) AS hash").columnAs("hash"));
        String value2 = Iterators.single(db.execute("MATCH ()-[r]->() RETURN apoc.hashing.fingerprintNodeOrRel(r) AS hash").columnAs("hash"));
        assertEquals(value, value2);
    }

    @Test
    public void fingerprintGraph() {
        compareGraph("CREATE (:Person{name:'ABC'})-[:KNOWS{since:12345}]->(:Person{name:'DEF'})", Collections.EMPTY_LIST, true);
    }

    @Test
    public void fingerprintGraphShouldFailUponDifferentProperties() {
        compareGraph("CREATE (:Person{name:'ABC', created:timestamp()})", Collections.EMPTY_LIST, false);
    }

    @Test
    public void testExcludes() {
        compareGraph("CREATE (:Person{name:'ABC', created:timestamp()})", Collections.singletonList("created"), true);
    }

    private void compareGraph(String cypher, List<String> excludes, boolean shouldBeEqual) {
        GraphDatabaseService db = neo4j.getGraphDatabaseService();
        Map<String, Object> params = Collections.singletonMap("excludes", excludes);

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
