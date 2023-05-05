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
package apoc.coll;

import apoc.Extended;
import org.apache.commons.collections4.CollectionUtils;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;
import org.neo4j.values.storable.DurationValue;

import java.time.temporal.ChronoUnit;
import java.util.List;

@Extended
public class CollFull {

    @UserFunction
    @Description("apoc.coll.avgDuration([duration('P2DT3H'), duration('PT1H45S'), ...]) -  returns the average of a list of duration values")
    public DurationValue avgDuration(@Name("durations") List<DurationValue> list) {
        if (CollectionUtils.isEmpty(list)) return null;

        long count = 0;

        double monthsRunningAvg = 0;
        double daysRunningAvg = 0;
        double secondsRunningAvg = 0;
        double nanosRunningAvg = 0;
        for (DurationValue duration : list) {
            count++;
            monthsRunningAvg += (duration.get(ChronoUnit.MONTHS) - monthsRunningAvg) / count;
            daysRunningAvg  += (duration.get(ChronoUnit.DAYS) - daysRunningAvg) / count;
            secondsRunningAvg  += (duration.get(ChronoUnit.SECONDS) - secondsRunningAvg) / count;
            nanosRunningAvg  += (duration.get(ChronoUnit.NANOS) - nanosRunningAvg) / count;
        }

        return DurationValue.approximate(monthsRunningAvg, daysRunningAvg, secondsRunningAvg, nanosRunningAvg)
                .normalize();
    }
}
