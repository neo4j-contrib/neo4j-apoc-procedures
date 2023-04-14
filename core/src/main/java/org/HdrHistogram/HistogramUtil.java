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
package org.HdrHistogram;

/**
 * @author mh
 * @since 20.12.17
 */
public class HistogramUtil {
    public static DoubleHistogram toDoubleHistogram(Histogram source, int numberOfSignificantValueDigits) {
        DoubleHistogram doubles = new DoubleHistogram(numberOfSignificantValueDigits);
        // Do max value first, to avoid max value updates on each iteration:
        int otherMaxIndex = source.countsArrayIndex(source.getMaxValue());
        long count = source.getCountAtIndex(otherMaxIndex);
        doubles.recordValueWithCount(source.valueFromIndex(otherMaxIndex), count);

        // Record the remaining values, up to but not including the max value:
        for (int i = 0; i < otherMaxIndex; i++) {
            count = source.getCountAtIndex(i);
            if (count > 0) {
                doubles.recordValueWithCount(source.valueFromIndex(i), count);
            }
        }
        return doubles;
    }
}
