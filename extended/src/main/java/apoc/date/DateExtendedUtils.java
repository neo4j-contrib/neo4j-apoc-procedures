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
package apoc.date;

import java.util.concurrent.TimeUnit;

public class DateExtendedUtils {
    public static final String DEFAULT_FORMAT = "yyyy-MM-dd HH:mm:ss";

    public static TimeUnit unit(String unit) {
        if (unit == null) return TimeUnit.MILLISECONDS;

        switch (unit.toLowerCase()) {
            case "ms":
            case "milli":
            case "millis":
            case "milliseconds":
                return TimeUnit.MILLISECONDS;
            case "s":
            case "second":
            case "seconds":
                return TimeUnit.SECONDS;
            case "m":
            case "minute":
            case "minutes":
                return TimeUnit.MINUTES;
            case "h":
            case "hour":
            case "hours":
                return TimeUnit.HOURS;
            case "d":
            case "day":
            case "days":
                return TimeUnit.DAYS;
        }

        throw new IllegalArgumentException("The unit: " + unit + " is not correct");
    }
}
