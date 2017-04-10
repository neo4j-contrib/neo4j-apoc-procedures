package apoc.math;

import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

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
        public Number avg_x;
        public Number avg_y;
        public Number slope;
        public Output(Number r2, Number avg_x, Number avg_y, Number slope){
            this.r2 = r2;
            this.avg_x = avg_x;
            this.avg_y = avg_y;
            this.slope = slope;
        }
    }

    @Procedure(name = "apoc.math.regr", mode = Mode.READ)
    public Stream<Output> regr(@Name("label") String label,
                               @Name("property_y") String y, @Name("property_x") String x ) {

        SimpleRegression regr = new SimpleRegression(false);
        Number regr_avgx = 0;
        Number regr_avgy = 0;
        int count = 0;

        try (ResourceIterator it = db.findNodes(Label.label(label))) {
            while (it.hasNext()) {
                Node node = (Node)it.next();
                Object prop_x = node.getProperty(x, null);
                Object prop_y = node.getProperty(y, null);
                if(prop_x != null && prop_y != null)
                {
                    regr_avgx = regr_avgx.doubleValue()  + (Long)prop_x;
                    regr_avgy = regr_avgy.doubleValue() +  (Long)prop_y;
                    regr.addData((Long)prop_x, (Long)prop_y);
                    count++;
                }
            }
        }
        regr_avgx = regr_avgx.doubleValue() / count;
        regr_avgy = regr_avgy.doubleValue() / count;
        return Stream.of(new Output(regr.getRSquare(), regr_avgx, regr_avgy, regr.getSlope()));
    }
}
