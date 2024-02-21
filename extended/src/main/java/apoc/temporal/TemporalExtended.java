package apoc.temporal;

import apoc.Extended;
import apoc.util.Util;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

import java.util.Map;

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

        String operator = acceptAdjacentSpans ? "<=" : "<";
        String query = "RETURN ($start1 %1$s $end2) AND ($start2 %1$s $end1) AS value".formatted(operator);
        Map<String, Object> params = Map.of("start1", start1,
                "end1", end1,
                "start2", start2,
                "end2", end2);
        
        try (Result result = tx.execute(query, params)) {
            Object value = result.next().get("value");
            return  (Boolean) value;
        }
    }
    
}
