package apoc.agg;

import org.neo4j.procedure.*;

import java.util.ArrayList;
import java.util.List;

/**
 * @author mh
 * @since 18.12.17
 */
public class CollAggregation {
    @UserAggregationFunction("apoc.agg.nth")
    @Description("apoc.agg.nth(value,offset) - returns value of nth row (or -1 for last)")
    public NthFunction nthFunction() {
        return new NthFunction();
    }

    @UserAggregationFunction("apoc.agg.first")
    @Description("apoc.agg.first(value) - returns first value")
    public FirstFunction first() {
        return new FirstFunction();
    }

    @UserAggregationFunction("apoc.agg.last")
    @Description("apoc.agg.last(value) - returns last value")
    public LastFunction last() {
        return new LastFunction();
    }

    @UserAggregationFunction("apoc.agg.slice")
    @Description("apoc.agg.slice(value, start, length) - returns subset of non-null values, start is 0 based and length can be -1")
    public SliceFunction slice() {
        return new SliceFunction();
    }

    public static class NthFunction {

        private Object value;
        private int index;

        @UserAggregationUpdate
        public void nth(@Name("value") Object value, @Name("value") long target) {
            if (value != null) {
                if (target == index++ || target == -1) {
                    this.value = value;
                }
            }
        }

        @UserAggregationResult
        public Object result() {
            return value;
        }
    }

    public static class SliceFunction {

        private List<Object> values = new ArrayList<>();
        private int index;

        @UserAggregationUpdate
        public void nth(@Name("value") Object value, @Name(value = "from", defaultValue = "0") long from, @Name(value = "to", defaultValue = "-1") long len) {
            if (value != null) {
                if (index >= from && (len == -1 || index < from + len)) {
                    this.values.add(value);
                }
                index++;
            }
        }

        @UserAggregationResult
        public List<Object> result() {
            return values;
        }
    }

    public static class FirstFunction {
        private Object value;

        @UserAggregationUpdate
        public void first(@Name("value") Object value) {
            if (value != null && this.value == null) {
                this.value = value;
            }
        }

        @UserAggregationResult
        public Object result() {
            return value;
        }
    }

    public static class LastFunction {
        private Object value;

        @UserAggregationUpdate
        public void last(@Name("value") Object value) {
            if (value != null) {
                this.value = value;
            }
        }

        @UserAggregationResult
        public Object result() {
            return value;
        }
    }
}
