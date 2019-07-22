package apoc.algo;

import org.neo4j.procedure.Description;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author mh
 * @since 07.05.16
 */
public class Cliques {

    @Context
    public GraphDatabaseService db;

    @Deprecated
    @Procedure
    @Description("apoc.algo.cliques(minSize) YIELD clique - search the graph and return all maximal cliques at least at " +
            "large as the minimum size argument.")
    public Stream<CliqueResult> cliques(@Name( "minSize" ) Number size)
    {
        Map<Long, Node> nodesToSearchFrom = new HashMap<>();

        for(Node node : db.getAllNodes())
        {
            nodesToSearchFrom.put( node.getId(), node ); //area to investigate improving efficiency.
        }
        return find_clique( new HashMap<>( ), nodesToSearchFrom, new HashMap<>(  ) )
                .stream().filter(
                cliqueResult -> cliqueResult.clique.size() >= size.intValue() );
    }

    @Deprecated
    @Procedure
    @Description("apoc.algo.cliquesWithNode(startNode, minSize) YIELD clique - search the graph and return all maximal cliques that " +
            "are at least as large than the minimum size argument and contain this node ")
    public Stream<CliqueResult> cliquesWithNode(@Name( "startNode" ) Node startNode, @Name( "minSize" ) Number size)
    {
        HashMap<Long, Node> nodesToSearchFrom = new HashMap<>();
        nodesToSearchFrom.put( startNode.getId(), startNode);
        for(Relationship relationship : startNode.getRelationships())
        {
            Node otherNode = relationship.getOtherNode( startNode );
            nodesToSearchFrom.put( otherNode.getId(), otherNode);
        }
        return find_clique( new HashMap<>(), nodesToSearchFrom, new HashMap<>() ).stream().filter(
                cliqueResult -> cliqueResult.clique.size() >= size.intValue() );
    }
    private List<CliqueResult> find_clique(Map<Long, Node> potentialClique, Map<Long, Node> remainingNodes,
                                           Map<Long, Node>
                                                   skipNodes)
    {
        List<CliqueResult> cliques = new LinkedList<>(  );
        if(remainingNodes.size() == 0 && skipNodes.size() == 0 && potentialClique.size() > 0)
        {
            cliques.add( new CliqueResult( potentialClique ));
            return cliques;
        }

        for(Iterator<Map.Entry<Long,Node>> it = remainingNodes.entrySet().iterator(); it.hasNext();)
        {
            Node node = it.next().getValue();
            Map<Long, Node> newPotentialClique = new HashMap<>( potentialClique );
            newPotentialClique.put(node.getId(), node);

            Map<Long, Node> newRemainingNodes = new HashMap<>(  );
            Map<Long, Node> newSkipNodes = new HashMap<>(  );
            for(Relationship relationship : node.getRelationships())
            {
                Node sibling = relationship.getOtherNode( node );
                if( remainingNodes.get( sibling.getId()) != null)
                {
                    newRemainingNodes.put( sibling.getId(), sibling );
                }
                if( skipNodes.get( sibling.getId() ) != null)
                {
                    newSkipNodes.put( sibling.getId(), sibling );
                }
            }
            cliques.addAll( find_clique(newPotentialClique, newRemainingNodes, newSkipNodes) );
            it.remove();
            skipNodes.put( node.getId(), node );
        }
        return cliques;
    }

    public static class CliqueResult {
        // yield
        public List<Node> clique;

        public CliqueResult(Map<Long, Node> nodes)
        {
            this.clique = nodes.values().stream().collect( Collectors.toList());
        }
    }
}
