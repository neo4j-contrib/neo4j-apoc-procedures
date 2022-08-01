package apoc.algo;

import apoc.result.RelationshipResult;
import apoc.util.Util;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class Cover {

    @Context
    public Transaction tx;

    @Procedure
    @Description("apoc.algo.cover(nodes) yield rel - returns all relationships between this set of nodes")
    public Stream<RelationshipResult> cover(@Name("nodes") Object nodes) {
        Set<Node> nodeSet = Util.nodeStream(tx, nodes).collect(Collectors.toSet());
        /*return nodeSet.parallelStream()
                .flatMap(n ->
                        Util.inTx(db,() ->
                        StreamSupport.stream(n.getRelationships(Direction.OUTGOING)
                                .spliterator(),false)
                                .filter(r -> nodeSet.contains(r.getEndNode()))
                                .map(RelationshipResult::new)));*/
        // NB prallel approach doesn't work in 3.4 (see SubgraphTest.testSubgraphAllShouldContainExpectedNodesAndRels
        // so falling back to single threaded
        // TODO: consider using multithreading and maybe kernel API here
        return coverNodes(nodeSet).map(RelationshipResult::new);
    }

    // non-parallelized utility method for use by other procedures
    public static Stream<Relationship> coverNodes(Collection<Node> nodes) {
        return nodes.stream()
                .flatMap(n ->
                        StreamSupport.stream(n.getRelationships(Direction.OUTGOING)
                                .spliterator(),false)
                                .filter(r -> nodes.contains(r.getEndNode())));
    }
}
