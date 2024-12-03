package apoc.agg;

import apoc.map.Maps;
import apoc.util.TestUtil;
import apoc.util.collection.IteratorsExtended;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
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

        db.executeTransactionally("""
                CREATE (:Person { louvain: 596, neo4jImportId: "18349390", wcc: 48, lpa: 598, name: "aaa", another: 548}),
                    (:Person { louvain: 596, neo4jImportId: "18349390", wcc: 48, lpa: 598, name: "eee", another: 549}),
                    (:Person { louvain: 596, neo4jImportId: "18349390", wcc: 48, lpa: 598, name: "eee", another: 549}),
                    (:Person { louvain: 597, neo4jImportId: "18349391", wcc: 48, lpa: 598, name: "eee", another: 549}),
                    (:Person { louvain: 597, neo4jImportId: "18349392", wcc: 47, lpa: 596, name: "iii", another: 549}),
                    (:Person { louvain: 597, neo4jImportId: "18349393", wcc: 47, lpa: 596, name: "iii", another: 549}),
                    (:Person { louvain: 597, neo4jImportId: "18349394", wcc: 47, lpa: 596, name: "iii", another: 549}),
                    (:Person { louvain: 597, neo4jImportId: "18349393", wcc: 47, lpa: 596, name: "iii", another: 10}),
                    (:Person { louvain: 597, neo4jImportId: "18349394", wcc: 47, lpa: 596, name: "iii", another: 10})""");
    }

    @AfterClass
    public static void tearDown() {
        db.shutdown();
    }

    // similar to https://community.neo4j.com/t/listing-the-community-size-of-different-community-detection-algorithms-already-calculated/42895
    @Test
    public void testMultiStatsComparedWithCypherMultiAggregation() {
        List multiAggregationResult = db.executeTransactionally("""
                        MATCH (p:Person)
                        WITH p
                        CALL {
                            WITH p
                            MATCH (n:Person {louvain: p.louvain})
                            RETURN sum(p.louvain) AS sumLouvain, avg(p.louvain) AS avgLouvain, count(p.louvain) AS countLouvain
                        }
                        CALL {
                            WITH p
                            MATCH (n:Person {wcc: p.wcc})
                            RETURN sum(p.wcc) AS sumWcc, avg(p.wcc) AS avgWcc, count(p.wcc) AS countWcc
                        }
                        CALL {
                            WITH p
                            MATCH (n:Person {another: p.another})
                            RETURN sum(p.another) AS sumAnother, avg(p.another) AS avgAnother, count(p.another) AS countAnother
                        }
                        CALL {
                            WITH p
                            MATCH (lpa:Person {lpa: p.lpa})
                            RETURN sum(p.lpa) AS sumLpa, avg(p.lpa) AS avgLpa, count(p.lpa) AS countLpa
                        }
                        RETURN p.name,
                            sumLouvain, avgLouvain, countLouvain,
                            sumWcc, avgWcc, countWcc,
                            sumAnother, avgAnother, countAnother,
                            sumLpa, avgLpa, countLpa""", Map.of(),
                IteratorsExtended::asList);

        List multiStatsResult = db.executeTransactionally("""
                match (p:Person)
                with apoc.agg.multiStats(p, ["lpa","wcc","louvain", "another"]) as data
                match (p:Person)
                return p.name,
                    data.wcc[toString(p.wcc)].avg AS avgWcc,
                    data.louvain[toString(p.louvain)].avg AS avgLouvain,
                    data.lpa[toString(p.lpa)].avg AS avgLpa,
                    data.another[toString(p.another)].avg AS avgAnother,
                    data.another[toString(p.another)].count AS countAnother,
                    data.wcc[toString(p.wcc)].count AS countWcc,
                    data.louvain[toString(p.louvain)].count AS countLouvain,
                    data.lpa[toString(p.lpa)].count AS countLpa,
                    data.another[toString(p.another)].sum AS sumAnother,
                    data.wcc[toString(p.wcc)].sum AS sumWcc,
                    data.louvain[toString(p.louvain)].sum AS sumLouvain,
                    data.lpa[toString(p.lpa)].sum AS sumLpa
                """, Map.of(), IteratorsExtended::asList);

        assertEquals(multiAggregationResult, multiStatsResult);
        
    }
     
}
