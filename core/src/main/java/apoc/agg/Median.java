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

import org.neo4j.procedure.*;

import java.util.ArrayList;
import java.util.Collections;
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
