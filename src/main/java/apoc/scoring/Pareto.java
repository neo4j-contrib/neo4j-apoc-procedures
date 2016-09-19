package apoc.scoring;

import java.util.stream.Stream;

import org.neo4j.procedure.Description;
import apoc.result.DoubleResult;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

public class Pareto {
    @Context
    public GraphDatabaseService db;

    @Procedure
    @Description("apoc.scoring.pareto(10, 20, 100, 11) applies a Pareto scoring function over the inputs")
    public Stream<DoubleResult> pareto(
            final @Name("minimumThreshold") long minimumThreshold,
            final @Name("eightyPercentValue") long eightyPercentValue,
            final @Name("maximumValue") long maximumValue,
            final @Name("score") long score) {
        if (score < minimumThreshold) {
            return Stream.of( new DoubleResult( 0.0 ) );
        }
        else {
            double alpha = Math.log((double) 5) / eightyPercentValue;
            double exp = Math.exp(-alpha * score);

            return Stream.of(new DoubleResult( maximumValue * (1 - exp) ));
        }
    }


}

