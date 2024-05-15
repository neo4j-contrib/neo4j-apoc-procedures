package apoc.agg;

import apoc.Extended;
import apoc.util.Util;
import org.apache.commons.collections4.ListUtils;
import org.neo4j.graphdb.Entity;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserAggregationFunction;
import org.neo4j.procedure.UserAggregationResult;
import org.neo4j.procedure.UserAggregationUpdate;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static apoc.agg.AggregationUtil.updateAggregationValues;


@Extended
public class Rollup {
    public static final String NULL_ROLLUP = "[NULL]";
    
    @UserAggregationFunction("apoc.agg.rollup")
    @Description("apoc.agg.rollup(<ANY>, [groupKeys], [aggKeys])" +
                 "\n Emulate an Oracle/Mysql rollup command: `ROLLUP groupKeys, SUM(aggKey1), AVG(aggKey1), COUNT(aggKey1), SUM(aggKey2), AVG(aggKey2), ... `")
    public RollupFunction rollup() {
        return new RollupFunction();
    }
    
    public static class RollupFunction {
        // Function to generate all combinations of a list with "TEST" as a placeholder
        public static <T> List<List<T>> generateCombinationsWithPlaceholder(List<T> elements) {
            List<List<T>> result = new ArrayList<>();
            generateCombinationsWithPlaceholder(elements, 0, new ArrayList<>(), result);
            return result;
        }

        // Helper function for generating combinations recursively
        private static <T> void generateCombinationsWithPlaceholder(List<T> elements, int index, List<T> current, List<List<T>> result) {
            if (index == elements.size()) {
                result.add(new ArrayList<>(current));
                return;
            }

            current.add(elements.get(index));
            generateCombinationsWithPlaceholder(elements, index + 1, current, result);
            current.remove(current.size() - 1);

            // Add "NULL" as a combination placeholder
            current.add((T) NULL_ROLLUP);
            generateCombinationsWithPlaceholder(elements, index + 1, current, result);
            current.remove(current.size() - 1);
        }
        
        private final Map<String, Object> result = new HashMap<>();

        private final Map<List<Object>, Map<String, Number>> rolledUpData = new HashMap<>();
        private List<String> groupKeysRes = null;
        
        @UserAggregationUpdate
        public void aggregate(
                @Name("value") Object value,
                @Name(value = "groupKeys") List<String> groupKeys,
                @Name(value = "aggKeys") List<String> aggKeys,
                @Name(value = "config", defaultValue = "{}")  Map<String, Object> config) {

            boolean cube = Util.toBoolean(config.get("cube"));
            
            Entity entity = (Entity) value;
            
            if (groupKeys.isEmpty()) {
                return;
            }
            groupKeysRes = groupKeys;
            
            /*
            if true: 
                emulate the CUBE command: https://docs.oracle.com/cd/F49540_01/DOC/server.815/a68003/rollup_c.htm#32311
            else:
                emulate the ROLLUP command: https://docs.oracle.com/cd/F49540_01/DOC/server.815/a68003/rollup_c.htm#32084
             */
            if (cube) {
                List<List<String>> groupingSets = generateCombinationsWithPlaceholder(groupKeys);

                    for (List<String> groupKey : groupingSets) {
                        List<Object> partialKey = new ArrayList<>();
                        for (String column : groupKey) {
                            partialKey.add(((Entity) value).getProperty(column, NULL_ROLLUP));
                        }
                        if (!rolledUpData.containsKey(partialKey)) {
                            rolledUpData.put(partialKey, new HashMap<>());
                        }
                        rollupAggregationProperties(aggKeys, entity, partialKey);
                    }
                    
                return;
            }

            List<Object> groupKey = groupKeys.stream()
                    .map(i -> entity.getProperty(i, null))
                    .toList();

            for (int i = 0; i <= groupKey.size(); i++) {
                // add NULL_ROLLUP to remaining elements, 
                // e.g. `[<firstGroupKey>, `NULL_ROLLUP`, `NULL_ROLLUP`]`
                List<Object> partialKey = ListUtils.union(groupKey.subList(0, i), Collections.nCopies(groupKey.size() - i, NULL_ROLLUP));
                if (!rolledUpData.containsKey(partialKey)) {
                    rolledUpData.put(partialKey, new HashMap<>());
                }
                rollupAggregationProperties(aggKeys, entity, partialKey);
            }
        }

        private void rollupAggregationProperties(List<String> aggKeys, Entity entity, List<Object> partialKey) {
            Map<String, Number> partialResult = rolledUpData.get(partialKey);
            for(var aggKey: aggKeys) {
                if (!entity.hasProperty(aggKey)) {
                    continue;
                }
                
                Object property = entity.getProperty(aggKey);
                
                String countKey = "COUNT(%s)".formatted(aggKey);
                String sumKey = "SUM(%s)".formatted(aggKey);
                String avgKey = "AVG(%s)".formatted(aggKey);

                updateAggregationValues(partialResult, property, countKey, sumKey, avgKey);
            }
        }

        /**
         * Transform a Map.of(ListGroupKeys, MapOfAggResults) in a List of Map.of(AggResult + ListGroupKeyToMap)
         */
        @UserAggregationResult
        public Object result() {
            List<HashMap<String, Object>> list = rolledUpData.entrySet().stream()
                    .map(e -> {
                        HashMap<String, Object> map = new HashMap<>();
                        for (int i = 0; i < groupKeysRes.size(); i++) {
                            map.put(groupKeysRes.get(i), e.getKey().get(i));
                        }
                        map.putAll(e.getValue());
                        return map;
                    })
                    .sorted((m1, m2) -> {
                        for (String key : groupKeysRes) {
                            Object value1 = m1.get(key);
                            Object value2 = m2.get(key);
                            int cmp = compareValues(value1, value2);
                            if (cmp != 0) {
                                return cmp;
                            }
                        }
                        return 0;
                    })
                    .toList();
            
            return list;
        }

        /**
         * We use this instead of e.g. apoc.coll.sortMulti 
         * since we have to handle the NULL_ROLLUP values as well
         */
        private static int compareValues(Object value1, Object value2) {
            if (value1 == null && value2 == null) {
                return 0;
            } else if (value1 == null) {
                return 1;
            } else if (value2 == null) {
                return -1;
            } else if (NULL_ROLLUP.equals(value1) && NULL_ROLLUP.equals(value2)) {
                return 0;
            } else if (NULL_ROLLUP.equals(value1)) {
                return 1;
            } else if (NULL_ROLLUP.equals(value2)) {
                return -1;
            } else if (value1 instanceof Comparable && value2 instanceof Comparable) {
                try {
                    return ((Comparable<Object>) value1).compareTo(value2);
                } catch (Exception e) {
                    // e.g. different data types, like int and strings
                    return 0;
                }

            } else {
                return 0;
            }
        }
    }
}
