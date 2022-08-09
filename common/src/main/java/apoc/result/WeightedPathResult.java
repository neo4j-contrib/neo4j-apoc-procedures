package apoc.result;

import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphalgo.WeightedPath;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;

import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * @author mh
 * @since 12.05.16
 */
public class WeightedPathResult { // TODO: derive from PathResult when access to derived properties is fixed for yield
    public Path path;
    public double weight;

    public WeightedPathResult(WeightedPath weightedPath) {
        this.path = weightedPath;
        this.weight = weightedPath.weight();
    }

    public static Stream<WeightedPathResult> streamWeightedPathResult(Node startNode, Node endNode, PathFinder<WeightedPath> algo) {
        Iterable<WeightedPath> allPaths = algo.findAllPaths(startNode, endNode);
        return StreamSupport.stream(allPaths.spliterator(), false)
                .map(WeightedPathResult::new);
    }
}
