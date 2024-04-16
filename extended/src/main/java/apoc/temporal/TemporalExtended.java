package apoc.temporal;

import apoc.Extended;
import apoc.util.Util;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;
import org.neo4j.values.AnyValue;
import org.neo4j.values.Comparison;

import java.util.Map;

import static org.neo4j.values.AnyValues.TERNARY_COMPARATOR;

@Extended
public class TemporalExtended {
    public static final String ACCEPT_ADJACENT_KEY = "acceptAdjacentSpans";
    
    @Context
    public Transaction tx;
    
    @UserFunction("apoc.temporal.overlap")
    @Description("apoc.temporal.overlap(start1,end1,start2,end2,$config) - Check whether the two temporal spans (start1-end1 and start2-end2) overlap or not")
    public Boolean overlap(@Name("start1") Object start1, 
                           @Name("end1") Object end1, 
                           @Name("start2") Object start2, 
                           @Name("end2") Object end2,
                           @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        boolean acceptAdjacentSpans = Util.toBoolean(config.get(ACCEPT_ADJACENT_KEY));

        AnyValue startValue1 = ValueUtils.of(start1);
        AnyValue endValue2 = ValueUtils.of(end2);

        AnyValue startValue2 = ValueUtils.of(start2);
        AnyValue endValue1 = ValueUtils.of(end1);

        // The overlap formula is: `(start1 <= end2) && (start2 <= end1)`
        Comparison comparisonLeft = TERNARY_COMPARATOR.ternaryCompare(startValue1, endValue2);
        Comparison comparisonRight = TERNARY_COMPARATOR.ternaryCompare(startValue2, endValue1);
        
        // if object are incompatibles, the Comparison returns UNDEFINED
        if (comparisonLeft.equals(Comparison.UNDEFINED) || comparisonRight.equals(Comparison.UNDEFINED)) {
            return null;
        }

        int rangeLeft = comparisonLeft.value();
        int rangeRight = comparisonRight.value();
        if (acceptAdjacentSpans) {
            return rangeLeft <= 0 && rangeRight <= 0;
        }
        return rangeLeft < 0 && rangeRight < 0;
    }
    
}
