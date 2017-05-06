package apoc.math;

import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.procedure.*;

import java.util.stream.Stream;

/**
 * @author <a href="mailto:ali.arslan@rwth-aachen.de">AliArslan</a>
 */
public class Regression {
    @Context
    public GraphDatabaseService db;

    // Result class
    public static class Output {
        public Number r2;
        public Number avgX;
        public Number avgY;
        public Number slope;
        public Output(Number r2, Number avgX, Number avgY, Number slope){
            this.r2 = r2;
            this.avgX = avgX;
            this.avgY = avgY;
            this.slope = slope;
        }
    }

    @Procedure(name = "apoc.math.regr", mode = Mode.READ)
    @Description("apoc.math.regr(label, propertyY, propertyX) | It calculates the coefficient " +
            "of determination (R-squared) for the values of propertyY and propertyX in the " +
            "provided label")
    public Stream<Output> regr(@Name("label") String label,
                               @Name("propertyY") String y, @Name("propertyX") String x ) {

        SimpleRegression regr = new SimpleRegression(false);
        double regrAvgX = 0;
        double regrAvgY = 0;
        int count = 0;

        try (ResourceIterator it = db.findNodes(Label.label(label))) {
            while (it.hasNext()) {
                Node node = (Node)it.next();
                Object propX = node.getProperty(x, null);
                Object propY = node.getProperty(y, null);
                if(propX != null && propY != null)
                {
                    regrAvgX = regrAvgX + (Long)propX;
                    regrAvgY = regrAvgY +  (Long)propY;
                    regr.addData((Long)propX, (Long)propY);
                    count++;
                }
            }
        }
        regrAvgX = regrAvgX / count;
        regrAvgY = regrAvgY / count;
        return Stream.of(new Output(regr.getRSquare(), regrAvgX, regrAvgY, regr.getSlope()));
    }
}
