package apoc.result;

import org.neo4j.graphdb.Path;

/**
 * @author mh
 * @since 11.04.16
 */
public class PathResult {
    public Path path;

    public PathResult(Path path) {
        this.path = path;
    }
}
