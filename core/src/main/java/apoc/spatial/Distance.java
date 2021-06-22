package apoc.spatial;

import org.neo4j.procedure.Description;
import apoc.result.DistancePathResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.*;
import java.util.stream.Stream;

public class Distance {

    private static final String LATITUDE = "latitude";
    private static final String LONGITUDE = "longitude";

    @Context
    public GraphDatabaseService db;

    @Procedure
    @Description("apoc.spatial.sortByDistance(List<Path>) sort the given paths based on the geo informations (lat/long) in ascending order")
    public Stream<DistancePathResult> sortByDistance(@Name("paths")List<Path> paths) {
        return paths.size() > 0 ? sortPaths(paths).stream() : Stream.empty();
    }

    public SortedSet<DistancePathResult> sortPaths(List<Path> paths) {
        SortedSet<DistancePathResult> result = new TreeSet<DistancePathResult>();
        for (int i = 0; i <= paths.size()-1; ++i) {
            double d = getPathDistance(paths.get(i));
            result.add(new DistancePathResult(paths.get(i), d));
        }

        return result;
    }

    public double getPathDistance(Path path) {
        double distance = 0;
        List<Node> nodes = new ArrayList<>();
        for (Node node : path.nodes()) {
            checkNodeHasGeo(node);
            nodes.add(node);
        }

        for (int i = 1; i <= nodes.size()-1; ++i) {
            Node prev = nodes.get(i-1);
            Node curr = nodes.get(i);
            distance += getDistance(
                    (double) prev.getProperty(LATITUDE),
                    (double) prev.getProperty(LONGITUDE),
                    (double) curr.getProperty(LATITUDE),
                    (double) curr.getProperty(LONGITUDE)
            );
        }

        return distance;
    }

    public double getDistance(double lat1, double lon1, double lat2, double lon2) {
        double theta = lon1 - lon2;
        double dist = Math.sin(deg2rad(lat1)) * Math.sin(deg2rad(lat2)) + Math.cos(deg2rad(lat1)) * Math.cos(deg2rad(lat2)) * Math.cos(deg2rad(theta));
        dist = Math.acos(dist);
        dist = rad2deg(dist);
        dist = dist * 60 * 1.1515;

        return dist * 1.609344;
    }

    private double deg2rad(double deg) {
        return (deg * Math.PI / 180.0);
    }

    private double rad2deg(double rad) {
        return (rad * 180.0 / Math.PI);
    }

    private void checkNodeHasGeo(Node node) {
        if (!node.hasProperty(LATITUDE) || !node.hasProperty(LONGITUDE)) {
            throw new IllegalArgumentException(String.format("Node with id %s has invalid geo properties", node.getId()));
        }
    }

}
