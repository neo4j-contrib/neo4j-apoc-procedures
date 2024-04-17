package apoc.agg;

import apoc.map.Maps;
import apoc.util.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class MultiStatsTest {

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    @BeforeClass
    public static void setUp() {
        TestUtil.registerProcedure(db, Maps.class, MultiStats.class);

        db.executeTransactionally(
                "CREATE (:Person { louvain: 596, neo4jImportId: \"18349390\", wcc: 48, lpa: 598, name: \"aaa\", another: 548}),\n" +
                "(:Person { louvain: 596, neo4jImportId: \"18349390\", wcc: 48, lpa: 598, name: \"eee\", another: 549}),\n" +
                "(:Person { louvain: 596, neo4jImportId: \"18349390\", wcc: 48, lpa: 598, name: \"eee\", another: 549}),\n" +
                "(:Person { louvain: 597, neo4jImportId: \"18349391\", wcc: 48, lpa: 598, name: \"eee\", another: 549}),\n" +
                "(:Person { louvain: 597, neo4jImportId: \"18349392\", wcc: 47, lpa: 596, name: \"iii\", another: 549}),\n" +
                "(:Person { louvain: 597, neo4jImportId: \"18349393\", wcc: 47, lpa: 596, name: \"iii\", another: 549}),\n" +
                "(:Person { louvain: 597, neo4jImportId: \"18349394\", wcc: 47, lpa: 596, name: \"iii\", another: 549}),\n" +
                "(:Person { louvain: 597, neo4jImportId: \"18349393\", wcc: 47, lpa: 596, name: \"iii\", another: 10}),\n" +
                "(:Person { louvain: 597, neo4jImportId: \"18349394\", wcc: 47, lpa: 596, name: \"iii\", another: 10})"
        );
    }

    @AfterClass
    public static void tearDown() {
        db.shutdown();
    }

    // similar to https://community.neo4j.com/t/listing-the-community-size-of-different-community-detection-algorithms-already-calculated/42895
    @Test
    public void testMultiStatsComparedWithCypherMultiAggregation() {
        String queryWithoutMultiStats = "MATCH (p:Person)\n" +
                       "WITH p\n" +
                       "CALL {\n" +
                       "    WITH p\n" +
                       "    MATCH (n:Person {louvain: p.louvain})\n" +
                       "    RETURN sum(p.louvain) AS sumLouvain, avg(p.louvain) AS avgLouvain, count(p.louvain) AS countLouvain\n" +
                       "}\n" +
                       "CALL {\n" +
                       "    WITH p\n" +
                       "    MATCH (n:Person {wcc: p.wcc})\n" +
                       "    RETURN sum(p.wcc) AS sumWcc, avg(p.wcc) AS avgWcc, count(p.wcc) AS countWcc\n" +
                       "}\n" +
                       "CALL {\n" +
                       "    WITH p\n" +
                       "    MATCH (n:Person {another: p.another})\n" +
                       "    RETURN sum(p.another) AS sumAnother, avg(p.another) AS avgAnother, count(p.another) AS countAnother\n" +
                       "}\n" +
                       "CALL {\n" +
                       "    WITH p\n" +
                       "    MATCH (lpa:Person {lpa: p.lpa})\n" +
                       "    RETURN sum(p.lpa) AS sumLpa, avg(p.lpa) AS avgLpa, count(p.lpa) AS countLpa\n" +
                       "}\n" +
                       "RETURN p.name,\n" +
                       "    sumLouvain, avgLouvain, countLouvain,\n" +
                       "    sumWcc, avgWcc, countWcc,\n" +
                       "    sumAnother, avgAnother, countAnother,\n" +
                       "    sumLpa, avgLpa, countLpa";
        
        List multiAggregationResult = db.executeTransactionally(queryWithoutMultiStats, Map.of(),
                Iterators::asList);

        String queryWithMultiStats = "match (p:Person)\n" +
                        "with apoc.agg.multiStats(p, [\"lpa\",\"wcc\",\"louvain\", \"another\"]) as data\n" +
                        "match (p:Person)\n" +
                        "return p.name,\n" +
                        "    data.wcc[toString(p.wcc)].avg AS avgWcc,\n" +
                        "    data.louvain[toString(p.louvain)].avg AS avgLouvain,\n" +
                        "    data.lpa[toString(p.lpa)].avg AS avgLpa,\n" +
                        "    data.another[toString(p.another)].avg AS avgAnother,\n" +
                        "    data.another[toString(p.another)].count AS countAnother,\n" +
                        "    data.wcc[toString(p.wcc)].count AS countWcc,\n" +
                        "    data.louvain[toString(p.louvain)].count AS countLouvain,\n" +
                        "    data.lpa[toString(p.lpa)].count AS countLpa,\n" +
                        "    data.another[toString(p.another)].sum AS sumAnother,\n" +
                        "    data.wcc[toString(p.wcc)].sum AS sumWcc,\n" +
                        "    data.louvain[toString(p.louvain)].sum AS sumLouvain,\n" +
                        "    data.lpa[toString(p.lpa)].sum AS sumLpa";
        
        List multiStatsResult = db.executeTransactionally(queryWithMultiStats, Map.of(),
                Iterators::asList);

        assertEquals(multiAggregationResult, multiStatsResult);
        
    }
     
}
