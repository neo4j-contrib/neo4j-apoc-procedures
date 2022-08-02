package apoc.math;

import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.procedure.*;

import java.util.stream.Stream;

/**
 * @author <a href="mailto:ali.arslan@rwth-aachen.de">AliArslan</a>
 */
public class Regression {
    @Context
    public Transaction tx;

    // Result class
    public static class Output {
        public double r2;
        public double avgX;
        public double avgY;
        public double slope;

        public Output(double r2, double avgX, double avgY, double slope) {
            this.r2 = r2;
            this.avgX = avgX;
            this.avgY = avgY;
            this.slope = slope;
        }
    }

    @Procedure(name = "apoc.math.regr", mode = Mode.READ)
    @Description("apoc.math.regr(label, propertyY, propertyX) - It calculates the coefficient " +
            "of determination (R-squared) for the values of propertyY and propertyX in the " +
            "provided label")
    public Stream<Output> regr(@Name("label") String label,
                               @Name("propertyY") String y, @Name("propertyX") String x) {

        SimpleRegression regr = new SimpleRegression(false);
        double regrAvgX = 0;
        double regrAvgY = 0;
        int count = 0;

        try (ResourceIterator it = tx.findNodes(Label.label(label))) {
            while (it.hasNext()) {
                Node node = (Node) it.next();
                Number propX = (Number) node.getProperty(x, null);
                Number propY = (Number) node.getProperty(y, null);
                if (propX != null && propY != null) {
                    regrAvgX = regrAvgX + propX.doubleValue();
                    regrAvgY = regrAvgY + propY.doubleValue();
                    regr.addData(propX.doubleValue(), propY.doubleValue());
                    count++;
                }
            }
        }
        regrAvgX = regrAvgX / count;
        regrAvgY = regrAvgY / count;
        return Stream.of(new Output(regr.getRSquare(), regrAvgX, regrAvgY, regr.getSlope()));
    }
}
