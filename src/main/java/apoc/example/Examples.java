package apoc.example;

import org.neo4j.procedure.Description;
import apoc.result.ProgressInfo;
import apoc.util.Util;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.QueryStatistics;
import org.neo4j.graphdb.Result;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Procedure;

import java.util.stream.Stream;

/**
 * @author mh
 * @since 24.05.16
 */
public class Examples {

    public
    @Context
    GraphDatabaseService db;

    @Procedure(mode = Mode.WRITE)
    @Description("apoc.example.movies() | Creates the sample movies graph")
    public Stream<ProgressInfo> movies() {
        long start = System.currentTimeMillis();
        String file = "movies.cypher";
        Result result = db.execute(Util.readResourceFile(file));
        QueryStatistics stats = result.getQueryStatistics();
        ProgressInfo progress = new ProgressInfo(file, "example movie database from themoviedb.org", "cypher")
                .update(stats.getNodesCreated(), stats.getRelationshipsCreated(), stats.getPropertiesSet())
                .done(start);
        result.close();
        return Stream.of(progress);
    }
}
