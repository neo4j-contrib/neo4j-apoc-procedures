package apoc.agg;

import org.HdrHistogram.DoubleHistogram;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.HistogramUtil;
import org.neo4j.procedure.*;

import java.util.ArrayList;
import java.util.List;

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
        private DoubleHistogram doubles;
        private List<Double> percentiles = asList(0.5D,0.75D,0.9D,0.95D,0.9D,0.99D);

        @UserAggregationUpdate
        public void aggregate(@Name("value") Number value, @Name(value = "percentiles", defaultValue = "[0.5,0.75,0.9,0.95,0.99]") List<Double> percentiles) {
            if (value != null) {
                if (doubles!=null) {
                    doubles.recordValue(value.doubleValue());
                } else if (value instanceof Double || value instanceof Float) {
                    this.doubles = HistogramUtil.toDoubleHistogram(values, 5);
                    doubles.recordValue(value.doubleValue());
                    values = null;
                } else {
                    values.recordValue(value.longValue());
                }
            }
            this.percentiles = percentiles;
        }

        @UserAggregationResult
        public List<Number> result() {
            long totalCount = values != null ? values.getTotalCount() : doubles.getTotalCount();
            boolean empty = totalCount == 0;
            List<Number> result = new ArrayList<>(percentiles.size());
            for (Double percentile : percentiles) {
                if (percentile == null || empty) {
                    result.add(null);
                } else {
                    if (values != null) {
                        result.add(values.getValueAtPercentile(percentile * 100D));
                    } else {
                        result.add(doubles.getValueAtPercentile(percentile * 100D));
                    }
                }
            }
            return result;
        }
    }
}
