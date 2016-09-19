package apoc.scoring;

import java.util.stream.Stream;

import org.neo4j.procedure.Description;
import apoc.result.DoubleResult;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

public class Existence
{
    @Context
    public GraphDatabaseService db;

    @Procedure
    @Description("apoc.scoring.existence(5, true) returns the provided score if true, 0 if false")
    public Stream<DoubleResult> existence(
            final @Name("score") long score,
            final @Name("exists") boolean exists) {
        return Stream.of( new DoubleResult( (double) (exists ? score : 0) ) );
    }


}

