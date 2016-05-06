package apoc.result;

import org.neo4j.graphdb.Path;

public class DistancePathResult implements Comparable<DistancePathResult> {

    public final Path path;

    public final double distance;

    public DistancePathResult(Path path, double distance) {
        this.path = path;
        this.distance = distance;
    }

    @Override
    public int compareTo(DistancePathResult o) {
        return o.distance < this.distance ? 1 : -1;
    }
}
