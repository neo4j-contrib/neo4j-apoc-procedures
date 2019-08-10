package apoc.generate;

import apoc.generate.config.ErdosRenyiConfig;
import apoc.generate.relationship.ErdosRenyiRelationshipGenerator;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

/**
 * Large integration test for {@link Neo4jGraphGenerator} with
 * {@link ErdosRenyiRelationshipGenerator}.
 */
public class ErdosRenyiGeneratorLargeTest {


    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();

    @Test(timeout = 60 * 1000)
    @Ignore("very long running test")
    public void shouldGenerateRelationshipsForLargeGraphInAReasonableAmountOfTime() {
        new ErdosRenyiRelationshipGenerator(new ErdosRenyiConfig(500_000, 10_000_000)).generateEdges();
    }

    @Test(timeout = 60 * 1000)
    @Ignore("very long running test")
    public void shouldGenerateRelationshipsForLargeGraphInAReasonableAmountOfTime2() {
        new ErdosRenyiRelationshipGenerator(new ErdosRenyiConfig(10000, 25_000_000)).generateEdges();
    }

}
