package apoc.algo;

import org.neo4j.procedure.Description;
import apoc.get.Get;
import apoc.path.PathExplorer;
import apoc.path.RelationshipTypeAndDirections;
import apoc.result.PathResult;
import apoc.result.RelationshipResult;
import apoc.result.WeightedPathResult;
import apoc.util.Util;
import org.neo4j.graphalgo.CommonEvaluators;
import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphalgo.WeightedPath;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.traversal.BranchState;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.neo4j.graphdb.traversal.Evaluators.atDepth;

public class Cover {

    @Context
    public GraphDatabaseAPI db;

    @Procedure
    @Description("apoc.algo.cover(nodes) yield rel - returns all relationships between this set of nodes")
    public Stream<RelationshipResult> cover(@Name("nodes") Object nodes) {
        Set<Node> nodeSet = Util.nodeStream(db, nodes).collect(Collectors.toSet());
        return nodeSet.parallelStream()
                .flatMap(n ->
                        Util.inTx(db,() ->
                        StreamSupport.stream(n.getRelationships(Direction.OUTGOING)
                                .spliterator(),false)
                                .filter(r -> nodeSet.contains(r.getEndNode()))
                                .map(RelationshipResult::new)));
    }
}
