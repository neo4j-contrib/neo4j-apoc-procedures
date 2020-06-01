package apoc.scoring;

import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

public class Scoring {
    @UserFunction
    @Description("apoc.scoring.existence(5, true) returns the provided score if true, 0 if false")
    public double existence(
            final @Name("score") long score,
            final @Name("exists") boolean exists) {
        return (double) (exists ? score : 0);
    }

    @UserFunction
    @Description("apoc.scoring.pareto(10, 20, 100, 11) applies a Pareto scoring function over the inputs")
    public double pareto(
            final @Name("minimumThreshold") long minimumThreshold,
            final @Name("eightyPercentValue") long eightyPercentValue,
            final @Name("maximumValue") long maximumValue,
            final @Name("score") long score) {
        if (score < minimumThreshold) {
            return 0.0d;
        }
        else {
            double alpha = Math.log((double) 5) / eightyPercentValue;
            double exp = Math.exp(-alpha * score);

            return maximumValue * (1 - exp);
        }
    }
}

