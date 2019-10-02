package apoc.algo;

import apoc.number.Numbers;
import apoc.util.TestUtil;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import static apoc.util.TestUtil.testResult;
import static org.junit.Assert.assertEquals;

public class SimilarityTest {
    private static final String SETUP = "create (java:Skill{name:'Java'})\n" +
            "create (neo4j:Skill{name:'Neo4j'})\n" +
            "create (nodejs:Skill{name:'NodeJS'})\n" +
            "create (scala:Skill{name:'Scala'})\n" +
            "create (jim:Employee{name:'Jim'})\n" +
            "create (bob:Employee{name:'Bob'})\n" +
            "create (role:Role {name:'Role 1-Analytics Manager'})\n" +
            "\n" +
            "create (role)-[:REQUIRES_SKILL{proficiency:8.54}]->(java)\n" +
            "create (role)-[:REQUIRES_SKILL{proficiency:4.3}]->(scala)\n" +
            "create (role)-[:REQUIRES_SKILL{proficiency:9.75}]->(neo4j)\n" +
            "\n" +
            "create (bob)-[:HAS_SKILL{proficiency:10}]->(java)\n" +
            "create (bob)-[:HAS_SKILL{proficiency:7.5}]->(neo4j)\n" +
            "create (bob)-[:HAS_SKILL]->(scala)\n" +
            "create (jim)-[:HAS_SKILL{proficiency:8.25}]->(java)\n" +
            "create (jim)-[:HAS_SKILL{proficiency:7.1}]->(scala)";

    // cosine similarity taken from here: https://neo4j.com/graphgist/a7c915c8-a3d6-43b9-8127-1836fecc6e2f
    // euclid distance taken from here: https://neo4j.com/blog/real-time-recommendation-engine-data-science/
    // euclid similarity taken from here: http://stats.stackexchange.com/a/158285

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();


    @Before
    public void setUp() throws Exception {
        TestUtil.registerProcedure(db, Similarity.class, Numbers.class);
        db.executeTransactionally(SETUP);
    }

    @Test
    public void testCosineSimilarityWithSomeWeightPropertiesNull() throws Exception {
        String controlQuery =
                "MATCH (p1:Employee)-[x:HAS_SKILL]->(sk:Skill)<-[y:REQUIRES_SKILL] -(p2:Role {name:'Role 1-Analytics Manager'})\n" +
                "WITH SUM(x.proficiency * y.proficiency) AS xyDotProduct,\n" +
                "SQRT(REDUCE(xDot = 0.0, a IN COLLECT(x.proficiency) | xDot + a^2)) AS xLength,\n" +
                "SQRT(REDUCE(yDot = 0.0, b IN COLLECT(y.proficiency) | yDot + b^2)) AS yLength,\n" +
                "p1, p2\n" +
                "WITH  p1.name as name, xyDotProduct / (xLength * yLength) as cosineSim\n" +
                "ORDER BY name ASC\n" +
                "RETURN name, apoc.number.format(cosineSim, '0.####') as cosineSim";
        String bobSimilarity;
        String jimSimilarity;

        try (Transaction tx = db.beginTx()) {
            Result result = tx.execute(controlQuery);
            bobSimilarity = (String) result.next().get("cosineSim");
            jimSimilarity = (String) result.next().get("cosineSim");
        }

        testResult(db,
                "MATCH (p1:Employee)-[x:HAS_SKILL]->(sk:Skill)<-[y:REQUIRES_SKILL]-(p2:Role {name:'Role 1-Analytics Manager'})\n" +
                     "WITH p1, COLLECT(coalesce(x.proficiency, 0.0d)) as v1, COLLECT(coalesce(y.proficiency, 0.0d)) as v2\n" +
                     "WITH p1.name as name, apoc.algo.cosineSimilarity(v1, v2) as cosineSim ORDER BY name ASC\n" +
                     "RETURN name, apoc.number.format(cosineSim, '0.####') as cosineSim",
                result -> {
                    assertEquals(bobSimilarity, result.next().get("cosineSim"));
                    assertEquals(jimSimilarity, result.next().get("cosineSim"));
                }
        );
    }

    @Test
    public void testCosineSimilarityWithSomeRelationshipsNull() throws Exception {
        String controlQuery =
                "MATCH (p1:Employee)\n" +
                "MATCH (sk:Skill)<-[y:REQUIRES_SKILL] -(p2:Role {name:'Role 1-Analytics Manager'})\n" +
                "OPTIONAL MATCH (p1)-[x:HAS_SKILL]->(sk)\n" +
                "WITH SUM(x.proficiency * y.proficiency) AS xyDotProduct,\n" +
                "SQRT(REDUCE(xDot = 0.0, a IN COLLECT(x.proficiency) | xDot + a^2)) AS xLength,\n" +
                "SQRT(REDUCE(yDot = 0.0, b IN COLLECT(y.proficiency) | yDot + b^2)) AS yLength,\n" +
                "p1, p2\n" +
                "WITH  p1.name as name, xyDotProduct / (xLength * yLength) as cosineSim\n" +
                "ORDER BY name ASC\n" +
                "RETURN name, apoc.number.format(cosineSim, '0.####') as cosineSim";
        String bobSimilarity;
        String jimSimilarity;
        try (Transaction tx = db.beginTx()) {
            Result result = tx.execute(controlQuery);
            bobSimilarity = (String) result.next().get("cosineSim");
            jimSimilarity = (String) result.next().get("cosineSim");
        }

        testResult(db,
                "MATCH (sk:Skill)<-[y:REQUIRES_SKILL]-(p2:Role {name:'Role 1-Analytics Manager'})\n" +
                     "MATCH (p1:Employee)\n" +
                     "OPTIONAL MATCH (p1)-[x:HAS_SKILL]->(sk)\n" +
                     "WITH p1, COLLECT(coalesce(x.proficiency, 0.0d)) as v1, COLLECT(coalesce(y.proficiency, 0.0d)) as v2\n" +
                     "WITH p1.name as name, apoc.algo.cosineSimilarity(v1, v2) as cosineSim ORDER BY name ASC\n" +
                     "RETURN name, apoc.number.format(cosineSim, '0.####') as cosineSim",
                result -> {
                    assertEquals(bobSimilarity, result.next().get("cosineSim"));
                    assertEquals(jimSimilarity, result.next().get("cosineSim"));
                }
        );
    }

    @Test
    public void testEuclideanDistance() throws Exception {
        String controlQuery =
                "MATCH (p1:Employee)\n" +
                "MATCH (sk:Skill)<-[y:REQUIRES_SKILL] -(p2:Role {name:'Role 1-Analytics Manager'})\n" +
                "OPTIONAL MATCH (p1)-[x:HAS_SKILL]->(sk)\n" +
                "WITH SQRT(SUM((coalesce(x.proficiency,0) - coalesce(y.proficiency, 0))^2)) AS euclidDist, p1, p2\n" +
                "ORDER BY p1.name ASC\n" +
                "RETURN p1.name, apoc.number.format(euclidDist, '0.####') as euclidDist";
        String bobDist;
        String jimDist;
        try (Transaction tx = db.beginTx()) {
            Result result = tx.execute(controlQuery);
            bobDist = (String) result.next().get("euclidDist");
            jimDist = (String) result.next().get("euclidDist");
        }

        testResult(db,
                "MATCH (sk:Skill)<-[y:REQUIRES_SKILL]-(p2:Role {name:'Role 1-Analytics Manager'})\n" +
                        "MATCH (p1:Employee)\n" +
                        "OPTIONAL MATCH (p1)-[x:HAS_SKILL]->(sk)\n" +
                        "WITH p1, COLLECT(coalesce(x.proficiency, 0.0d)) as v1, COLLECT(coalesce(y.proficiency, 0.0d)) as v2\n" +
                        "WITH p1.name as name, apoc.algo.euclideanDistance(v1, v2) as euclidDist ORDER BY name ASC\n" +
                        "RETURN name, apoc.number.format(euclidDist, '0.####') as euclidDist",
                result -> {
                    assertEquals(bobDist, result.next().get("euclidDist"));
                    assertEquals(jimDist, result.next().get("euclidDist"));
                }
        );
    }

    @Test
    public void testEuclideanSimilarity() throws Exception {
        String controlQuery =
                "MATCH (p1:Employee)\n" +
                "MATCH (sk:Skill)<-[y:REQUIRES_SKILL] -(p2:Role {name:'Role 1-Analytics Manager'})\n" +
                "OPTIONAL MATCH (p1)-[x:HAS_SKILL]->(sk)\n" +
                "WITH SQRT(SUM((coalesce(x.proficiency,0) - coalesce(y.proficiency, 0))^2)) AS euclidDist, p1\n" +
                "WITH p1.name as name, 1 / (1 + euclidDist) as euclidSim\n" +
                "ORDER BY name ASC\n" +
                "RETURN name, apoc.number.format(euclidSim, '0.####') as euclidSim";
        String bobSim;
        String jimSim;
        try (Transaction tx = db.beginTx()) {
            Result result = tx.execute(controlQuery);
            bobSim = (String) result.next().get("euclidSim");
            jimSim = (String) result.next().get("euclidSim");
        }

        testResult(db,
                "MATCH (sk:Skill)<-[y:REQUIRES_SKILL]-(p2:Role {name:'Role 1-Analytics Manager'})\n" +
                        "MATCH (p1:Employee)\n" +
                        "OPTIONAL MATCH (p1)-[x:HAS_SKILL]->(sk)\n" +
                        "WITH p1, COLLECT(coalesce(x.proficiency, 0.0d)) as v1, COLLECT(coalesce(y.proficiency, 0.0d)) as v2\n" +
                        "WITH p1.name as name, apoc.algo.euclideanSimilarity(v1, v2) as euclidSim ORDER BY name ASC\n" +
                        "RETURN name, apoc.number.format(euclidSim, '0.####') as euclidSim",
                result -> {
                    assertEquals(bobSim, result.next().get("euclidSim"));
                    assertEquals(jimSim, result.next().get("euclidSim"));
                }
        );
    }
}
