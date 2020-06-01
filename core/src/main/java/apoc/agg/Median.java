package apoc.agg;

import org.neo4j.procedure.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * @author mh
 * @since 18.12.17
 */
public class Median {
    @UserAggregationFunction("apoc.agg.median")
    @Description("apoc.agg.median(number) - returns median for non-null numeric values")
    public MedianFunction median() {
        return new MedianFunction();
    }


    public static class MedianFunction {
        private List<Double> values = new ArrayList<>();

        @UserAggregationUpdate
        public void aggregate(@Name("value") Object value) {
            if (value instanceof Number) {
                values.add(((Number)value).doubleValue());
            }
        }

        @UserAggregationResult
        public Object result() {
            if (values.isEmpty()) return null;
            Collections.sort(values);
            int size = values.size();
            if (size % 2 == 1) {
                return values.get(size /2);
            } else {
                return (values.get(size /2-1) + values.get(size /2)) / 2D;
            }
        }
    }
}
