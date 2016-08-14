package apoc.generate;

import apoc.Description;
import apoc.generate.config.*;
import apoc.generate.node.DefaultNodeCreator;
import apoc.generate.node.NodeCreator;
import apoc.generate.node.SocialNetworkNodeCreator;
import apoc.generate.relationship.*;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.PerformsWrites;
import org.neo4j.procedure.Procedure;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class Generate {

    @Context
    public GraphDatabaseService db;

    @Procedure("apoc.generate.ba")
    @PerformsWrites
    @Description("apoc.generate.ba(noNodes, edgesPerNode, label, type) - generates a random graph according to the Barabasi-Albert model")
    public void barabasiAlbert(@Name("noNodes") Long noNodes, @Name("edgesPerNode") Long edgesPerNode, @Name("label") String label, @Name("type") String relationshipType) throws IOException {
        if (noNodes == null) noNodes = 1000L;
        if (edgesPerNode == null) edgesPerNode = 2L;

        BarabasiAlbertConfig barabasiAlbertConfig = new BarabasiAlbertConfig(noNodes.intValue(), edgesPerNode.intValue());
        RelationshipGenerator relationshipGenerator = new BarabasiAlbertRelationshipGenerator(barabasiAlbertConfig);

        generateGraph(relationshipGenerator, label, relationshipType);
    }

    @Procedure("apoc.generate.ws")
    @PerformsWrites
    @Description("apoc.generate.ws(noNodes, degree, beta, label, type) - generates a random graph according to the Watts-Strogatz model")
    public void wattsStrogatz(@Name("noNodes") Long noNodes, @Name("degree") Long degree, @Name("beta") Double beta, @Name("label") String label, @Name("type") String relationshipType) throws IOException {
        if (noNodes == null) noNodes = 1000L;
        if (degree == null) degree = 4L;
        if (beta == null) beta = 0.5;

        WattsStrogatzConfig wattsStrogatzConfig = new WattsStrogatzConfig(noNodes.intValue(), degree.intValue(), beta);
        RelationshipGenerator relationshipGenerator = new WattsStrogatzRelationshipGenerator(wattsStrogatzConfig);

        generateGraph(relationshipGenerator, label, relationshipType);
    }

    @Procedure("apoc.generate.er")
    @PerformsWrites
    @Description("apoc.generate.er(noNodes, noEdges, label, type) - generates a random graph according to the Erdos-Renyi model")
    public void erdosRenyi(@Name("noNodes") Long noNodes, @Name("noEdges") Long noEdges, @Name("label") String label, @Name("type") String relationshipType) throws IOException {
        if (noNodes == null) noNodes = 1000L;
        if (noEdges == null) noEdges = 10000L;

        ErdosRenyiConfig erdosRenyiConfig = new ErdosRenyiConfig(noNodes.intValue(), noEdges.intValue());
        RelationshipGenerator relationshipGenerator = new ErdosRenyiRelationshipGenerator(erdosRenyiConfig);

        generateGraph(relationshipGenerator, label, relationshipType);
    }

    @Procedure("apoc.generate.complete")
    @PerformsWrites
    @Description("apoc.generate.complete(noNodes, label, type) - generates a random complete graph")
    public void complete(@Name("noNodes") Long noNodes, @Name("label") String label, @Name("type") String relationshipType) throws IOException {
        if (noNodes == null) noNodes = 1000L;

        NumberOfNodesBasedConfig numberOfNodesBasedConfig = new NumberOfNodesBasedConfig(noNodes.intValue());
        RelationshipGenerator relationshipGenerator = new CompleteGraphRelationshipGenerator(numberOfNodesBasedConfig);

        generateGraph(relationshipGenerator, label, relationshipType);
    }

    @Procedure("apoc.generate.simple")
    @PerformsWrites
    @Description("apoc.generate.simple(degrees, label, type) - generates a simple random graph according to the given degree distribution")
    public void simple(@Name("degrees") List<Long> degrees, @Name("label") String label, @Name("type") String relationshipType) throws IOException {
        if (degrees == null) degrees = Arrays.asList(2L, 2L, 2L, 2L);

        List<Integer> intDegrees = degrees.stream().map(Long::intValue).collect(Collectors.toList());

        RelationshipGenerator relationshipGenerator = new SimpleGraphRelationshipGenerator(new DistributionBasedConfig(intDegrees));

        generateGraph(relationshipGenerator, label, relationshipType);
    }

    private void generateGraph(RelationshipGenerator relationshipGenerator, String label, String type) throws IOException {
        NodeCreator nodeCreator;
        if (label == null || "Person".equals(label)) {
            nodeCreator = new SocialNetworkNodeCreator();
        }
        else {
            nodeCreator = new DefaultNodeCreator(label);
        }

        RelationshipCreator relationshipCreator;
        if (type == null || "FRIEND_OF".equals(type)) {
            relationshipCreator = new SocialNetworkRelationshipCreator();
        }
        else {
            relationshipCreator = new DefaultRelationshipCreator(type);
        }

        GeneratorConfiguration configuration = new BasicGeneratorConfig(relationshipGenerator, nodeCreator, relationshipCreator);
        new Neo4jGraphGenerator(db).generateGraph(configuration);
    }
}
