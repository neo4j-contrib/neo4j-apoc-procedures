package apoc.algo;

import apoc.Description;
import apoc.path.RelationshipTypeAndDirections;
import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphalgo.WeightedPath;
import org.neo4j.graphdb.*;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class Algo {

    @Procedure
    @Description("apoc.algo.dijkstra(startNode, endNode, 'KNOWS|WORKS_WITH<|IS_MANAGER_OF>', 'distance') YIELD yield, weight - run dijkstra with relationship property name as cost function")
    public Stream<WeightedPathResult> dijkstra(
            @Name("startNode") Node startNode,
            @Name("endNode") Node endNode,
            @Name("relationshipTypesAndDirections") String relTypesAndDirs,
            @Name("weightPropertyName") String weightPropertyName) {

        PathFinder<WeightedPath> algo = GraphAlgoFactory.dijkstra(
                buildPathExpander(relTypesAndDirs),
                weightPropertyName
        );
        return streamWeightedPathResult(startNode, endNode, algo);
    }

    @Procedure
    @Description("apoc.algo.dijkstraWithDefaultWeight(startNode, endNode, 'KNOWS|WORKS_WITH<|IS_MANAGER_OF>', 'distance', 10) YIELD yield, weight - run dijkstra with relationship property name as cost function and a default weight if the property does not exist")
    public Stream<WeightedPathResult> dijkstraWithDefaultWeight(
            @Name("startNode") Node startNode,
            @Name("endNode") Node endNode,
            @Name("relationshipTypesAndDirections") String relTypesAndDirs,
            @Name("weightPropertyName") String weightPropertyName,
            @Name("defaultWeight") double defaultWeight) {

        PathFinder<WeightedPath> algo = GraphAlgoFactory.dijkstra(
                buildPathExpander(relTypesAndDirs),
                (relationship, direction) -> ((Number) (relationship.getProperty(weightPropertyName, defaultWeight))).doubleValue()
        );
        return streamWeightedPathResult(startNode, endNode, algo);
    }

    private PathExpander<Object> buildPathExpander(String relationshipsAndDirections) {
        PathExpanderBuilder builder = PathExpanderBuilder.empty();
        for (Pair<RelationshipType, Direction> pair : RelationshipTypeAndDirections.parse(relationshipsAndDirections)) {
            if (pair.first()==null) {
                builder = PathExpanderBuilder.allTypes(pair.other());
            } else {
                builder = builder.add(pair.first(), pair.other());
            }
        }
        return builder.build();
    }

    private Stream<WeightedPathResult> streamWeightedPathResult(@Name("startNode") Node startNode, @Name("endNode") Node endNode, PathFinder<WeightedPath> algo) {
        Iterable<WeightedPath> allPaths = algo.findAllPaths(startNode, endNode);
        return StreamSupport.stream(allPaths.spliterator(), false)
                .map(WeightedPathResult::new);
    }

    public static class WeightedPathResult { // TODO: derive from PathResult when access to derived properties is fixed for yield
        public Path path;
        public double weight;

        public WeightedPathResult(WeightedPath weightedPath) {
            this.path = weightedPath;
            this.weight = weightedPath.weight();
        }
    }
}
