package apoc.algo;

import apoc.number.Numbers;
import apoc.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.TestGraphDatabaseFactory;

import static apoc.util.TestUtil.testResult;
import static org.junit.Assert.assertEquals;


public class SimilarityTest {
    private static final String SETUP = "create (java:Personal_Skill{name:'Java'})\n" +
            "create (neo4j:Personal_Skill{name:'Neo4j'})\n" +
            "create (nodejs:Personal_Skill{name:'NodeJS'})\n" +
            "create (scala:Personal_Skill{name:'Scala'})\n" +
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

    private static final String BOB_SIMILARITY_AGAINST_MATCHED_SKILLS = "0.9287";
    private static final String JIM_SIMILARITY_AGAINST_MATCHED_SKILLS = "0.9703";
    private static final String BOB_SIMILARITY_AGAINST_REQUIRED_SKILLS = "0.9287";
    private static final String JIM_SIMILARITY_AGAINST_REQUIRED_SKILLS = "0.6794";

    private GraphDatabaseService db;

    @Before
    public void setUp() throws Exception {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        TestUtil.registerProcedure(db, Similarity.class);
        TestUtil.registerProcedure(db, Numbers.class);
        db.execute(SETUP).close();
    }

    @After
    public void tearDown() {
        db.shutdown();
    }

    @Test
    public void testSimilarityWithSomeWeightPropertiesNull() throws Exception {
        testResult(db,
                "MATCH (p1:Employee)-[x:HAS_SKILL]->(sk:Personal_Skill)<-[y:REQUIRES_SKILL]-(p2:Role {name:'Role 1-Analytics Manager'})\n" +
                     "WITH p1, COLLECT([x, y]) as skills\n" +
                     "WITH p1.name as name, apoc.algo.cosineSimilarity(skills, 'proficiency', 0.0) as cosineSim ORDER BY name ASC\n" +
                     "RETURN name, apoc.number.format(cosineSim, '0.####') as cosineSim",
                result -> {
                    assertEquals(BOB_SIMILARITY_AGAINST_MATCHED_SKILLS, result.next().get("cosineSim"));
                    assertEquals(JIM_SIMILARITY_AGAINST_MATCHED_SKILLS, result.next().get("cosineSim"));
                }
        );

    }

    @Test
    public void testSimilarityWithSomeRelationshipsNull() throws Exception {
        testResult(db,
                "MATCH (sk:Personal_Skill)<-[y:REQUIRES_SKILL]-(p2:Role {name:'Role 1-Analytics Manager'})\n" +
                     "MATCH (p1:Employee)\n" +
                     "OPTIONAL MATCH (p1)-[x:HAS_SKILL]->(sk)\n" +
                     "WITH p1, COLLECT([x, y]) as skillLevel\n" +
                     "WITH p1.name as name, apoc.algo.cosineSimilarity(skillLevel, 'proficiency', 0.0) as cosineSim ORDER BY name ASC\n" +
                     "RETURN name, apoc.number.format(cosineSim, '0.####') as cosineSim",
                result -> {
                    assertEquals(BOB_SIMILARITY_AGAINST_REQUIRED_SKILLS, result.next().get("cosineSim"));
                    assertEquals(JIM_SIMILARITY_AGAINST_REQUIRED_SKILLS, result.next().get("cosineSim"));
                }
        );
    }
}
