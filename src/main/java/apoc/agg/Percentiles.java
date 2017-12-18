package apoc.agg;

import org.HdrHistogram.Histogram;
import org.neo4j.procedure.*;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;

/**
 * @author mh
 * @since 18.12.17
 */
public class Percentiles {
    @UserAggregationFunction("apoc.agg.percentiles")
    @Description("apoc.agg.percentiles(value,[percentiles = 0.5,0.75,0.9,0.95,0.99]) - returns given percentiles for values")
    public PercentilesFunction percentiles() {
        return new PercentilesFunction();
    }


    public static class PercentilesFunction {

        private Histogram values = new Histogram(3);
        private List<Double> percentiles = asList(0.5D,0.75D,0.9D,0.95D,0.9D,0.99D);

        @UserAggregationUpdate
        public void nth(@Name("value") Number value, @Name(value = "percentiles", defaultValue = "[0.5,0.75,0.9,0.95,0.99]") List<Double> percentiles) {
            if (value != null) {
                values.recordValue(value.longValue());
            }
            this.percentiles = percentiles;
        }

        @UserAggregationResult
        public List<Number> result() {
            boolean empty = values.getTotalCount() == 0;
            List<Number> result = new ArrayList<>(percentiles.size());
            for (Double percentile : percentiles) {
                if (percentile == null || empty) {
                    result.add(null);
                } else {
                    result.add(values.getValueAtPercentile(percentile*100D));
                }
            }
            return result;
        }
    }
}
