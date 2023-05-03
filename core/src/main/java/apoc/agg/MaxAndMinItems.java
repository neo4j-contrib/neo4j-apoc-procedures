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

import apoc.util.Util;
import org.neo4j.procedure.*;

import java.util.ArrayList;
import java.util.List;

/**
 * Aggregation functions for collecting items with only the minimal or maximal values.
 * This is meant to replace queries like this:

 <pre>
 MATCH (p:Person)
 WHERE p.born &gt;= 1930
 WITH p.born as born, collect(p.name) as persons
 WITH min(born) as minBorn, collect({born:born, persons:persons}) as bornInfoList
 UNWIND [info in bornInfoList WHERE info.born = minBorn] as bornInfo
 RETURN bornInfo.born as born, [person in bornInfo.persons | person.name] as persons
 </pre>

 * with an aggregation like this:

 <pre>
 MATCH (p:Person)
 WHERE p.born &gt;= 1930
 WITH apoc.agg.minItems(p, p.born) as minResult
 RETURN minResult.value as born, [person in minResult.items | person.name] as persons
 </pre>

 * returns {born:1930, persons:["Gene Hackman", "Richard Harris", "Clint Eastwood"]}

 */
public class MaxAndMinItems {

    @UserAggregationFunction("apoc.agg.maxItems")
    @Description("apoc.agg.maxItems(item, value, groupLimit: -1) - returns a map {items:[], value:n} where `value` is the maximum value present, and `items` are all items with the same value. The number of items can be optionally limited.")
    public MaxOrMinItemsFunction maxItems() {
        return new MaxOrMinItemsFunction(true);
    }

    @UserAggregationFunction("apoc.agg.minItems")
    @Description("apoc.agg.minItems(item, value, groupLimit: -1) - returns a map {items:[], value:n} where `value` is the minimum value present, and `items` are all items with the same value. The number of items can be optionally limited.")
    public MaxOrMinItemsFunction minItems() {
        return new MaxOrMinItemsFunction(false);
    }


    public static class MaxOrMinItemsFunction {
        private final List<Object> items = new ArrayList<>();
        private final boolean isMax;
        private Comparable value;

        private MaxOrMinItemsFunction(boolean isMax) {
            this.isMax = isMax;
        }

        @UserAggregationUpdate
        public void maxOrMinItems(@Name("item") final Object item, @Name("value") final Object inputValue,
                                  @Name(value = "groupLimit", defaultValue = "-1") final Long groupLimitParam) {
            int groupLimit = groupLimitParam.intValue();
            boolean noGroupLimit = groupLimit < 0;

            if (item != null && inputValue != null) {
                int result = value == null ? (isMax ? -1 : 1) : value.compareTo(inputValue);
                if (result == 0) {
                    if (noGroupLimit || items.size() < groupLimit) {
                        items.add(item);
                    }
                } else if (result < 0 == isMax) {
                    // xnor logic, interested value should replace current value
                    items.clear();
                    items.add(item);
                    value = (Comparable) inputValue;
                }
            }
        }

        @UserAggregationResult
        public Object result() {
            return Util.map("items", items, "value", value);
        }
    }
}
