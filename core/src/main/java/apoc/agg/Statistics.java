/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package apoc.agg;

import static java.util.Arrays.asList;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.HdrHistogram.DoubleHistogram;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.HistogramUtil;
import org.neo4j.procedure.*;

/**
 * @author mh
 * @since 18.12.17
 */
public class Statistics {
    @UserAggregationFunction("apoc.agg.statistics")
    @Description(
            "apoc.agg.statistics(value,[percentiles = 0.5,0.75,0.9,0.95,0.99]) - returns numeric statistics (percentiles, min,minNonZero,max,total,mean,stdev) for values")
    public StatisticsFunction statistics() {
        return new StatisticsFunction();
    }

    public static class StatisticsFunction {

        private Histogram values = new Histogram(3);
        private DoubleHistogram doubles;
        private List<Double> percentiles = asList(0.5D, 0.75D, 0.9D, 0.95D, 0.9D, 0.99D);
        private Number minValue;
        private Number maxValue;

        @UserAggregationUpdate
        public void aggregate(
                @Name("value") Number value,
                @Name(value = "percentiles", defaultValue = "[0.5,0.75,0.9,0.95,0.99]") List<Double> percentiles) {
            if (value != null) {
                if (doubles != null) {
                    doubles.recordValue(value.doubleValue());
                } else if (value instanceof Double || value instanceof Float) {
                    this.doubles = HistogramUtil.toDoubleHistogram(values, 5);
                    doubles.recordValue(value.doubleValue());
                    values = null;
                } else {
                    values.recordValue(value.longValue());
                }
                if (minValue == null || minValue.doubleValue() > value.doubleValue()) {
                    minValue = value;
                }
                if (maxValue == null || maxValue.doubleValue() < value.doubleValue()) {
                    maxValue = value;
                }
            }
            this.percentiles = percentiles;
        }

        @UserAggregationResult
        public Map<String, Number> result() {
            long totalCount = values != null ? values.getTotalCount() : doubles.getTotalCount();
            boolean empty = totalCount == 0;
            Map<String, Number> result = new LinkedHashMap<>(percentiles.size() + 6);
            result.put("min", minValue);
            result.put("minNonZero", values != null ? values.getMinNonZeroValue() : doubles.getMinNonZeroValue());
            result.put("max", maxValue);
            result.put("total", totalCount);
            result.put("mean", values != null ? values.getMean() : doubles.getMean());
            result.put("stdev", values != null ? values.getStdDeviation() : doubles.getStdDeviation());

            for (Double percentile : percentiles) {
                if (percentile != null && !empty) {
                    if (values != null) {
                        result.put(percentile.toString(), values.getValueAtPercentile(percentile * 100D));
                    } else {
                        result.put(percentile.toString(), doubles.getValueAtPercentile(percentile * 100D));
                    }
                }
            }
            return result;
        }
    }
}
