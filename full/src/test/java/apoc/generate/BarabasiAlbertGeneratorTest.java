package apoc.generate;

import apoc.generate.config.BarabasiAlbertConfig;
import apoc.generate.config.BasicGeneratorConfig;
import apoc.generate.node.SocialNetworkNodeCreator;
import apoc.generate.relationship.BarabasiAlbertRelationshipGenerator;
import apoc.generate.relationship.SocialNetworkRelationshipCreator;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * Integration test for {@link Neo4jGraphGenerator} with {@link BarabasiAlbertRelationshipGenerator}.
 */
public class BarabasiAlbertGeneratorTest {

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();

    @Test
    public void shouldGeneratePowerLawDistribution() {
        new Neo4jGraphGenerator(db).generateGraph(new BasicGeneratorConfig(
                new BarabasiAlbertRelationshipGenerator(new BarabasiAlbertConfig(100, 2)),
                new SocialNetworkNodeCreator(),
                new SocialNetworkRelationshipCreator()
        ));

        List<Integer> degrees = new LinkedList<>();

        try (Transaction tx = db.beginTx()) {
            for (Node node : tx.getAllNodes()) {
                degrees.add(node.getDegree());
            }
            tx.commit();
        }

        Collections.sort(degrees, Collections.reverseOrder());

        //TODO make this an automated test
        //System.out.println(ArrayUtils.toString(degrees.toArray(new Integer[degrees.size()])));
    }

    @Test(timeout = 10 * 1000)
    @Ignore
    public void shouldGenerateRelationshipsForLargeGraphInAReasonableAmountOfTime() {
        new BarabasiAlbertRelationshipGenerator(new BarabasiAlbertConfig(1_000_000, 3)).generateEdges();
    }

}
