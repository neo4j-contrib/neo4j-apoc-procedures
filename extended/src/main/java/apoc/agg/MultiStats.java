package apoc.agg;

import apoc.Extended;
import org.neo4j.graphdb.Entity;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserAggregationFunction;
import org.neo4j.procedure.UserAggregationResult;
import org.neo4j.procedure.UserAggregationUpdate;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static apoc.agg.AggregationUtil.updateAggregationValues;

@Extended
public class MultiStats {

    @UserAggregationFunction("apoc.agg.multiStats")
    @Description("Return a multi-dimensional aggregation")
    public MultiStatsFunction multiStats() {
        return new MultiStatsFunction();
    }

    public static class MultiStatsFunction {

        private final Map<String, Map<String, Map<String, Number>>> result = new HashMap<>();
        
        @UserAggregationUpdate
        public void aggregate(
                @Name("value") Object value,
                @Name(value = "keys") List<String> keys) {
            Entity entity = (Entity) value; 
            
            // for each prop
            keys.forEach(key -> {
                if (entity.hasProperty(key)) {
                    Object property = entity.getProperty(key);
                    
                    result.compute(key, (ignored, v) -> {
                        Map<String, Map<String, Number>> map = Objects.requireNonNullElseGet(v, HashMap::new);
                        
                        map.compute(property.toString(), (propKey, propVal) -> {

                            Map<String, Number> propMap = Objects.requireNonNullElseGet(propVal, HashMap::new);

                            String countKey = "count";
                            String sumKey = "sum";
                            String avgKey = "avg";

                            updateAggregationValues(propMap, property, countKey, sumKey, avgKey);

                            return propMap;
                        });

                        return map;
                    });
                }
            });
        }
        

        @UserAggregationResult
        // apoc.agg.multiStats([key1,key2,key3]) -> Map<Key,Map<agg="sum,count,avg", number>>
        public Map<String, Map<String, Map<String, Number>>> result() {
            return result;
        }
    }
}
