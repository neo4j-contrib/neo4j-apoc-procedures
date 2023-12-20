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
package apoc.util;

import static apoc.util.DateFormatUtil.ELASTIC_PATTERNS;
import static apoc.util.DateFormatUtil.ISO_DURATION_PATTERNS;
import static org.neo4j.values.storable.DurationFields.DAYS;
import static org.neo4j.values.storable.DurationFields.HOURS;
import static org.neo4j.values.storable.DurationFields.MILLISECONDS;
import static org.neo4j.values.storable.DurationFields.MINUTES_OF_HOUR;
import static org.neo4j.values.storable.DurationFields.MONTHS_OF_YEAR;
import static org.neo4j.values.storable.DurationFields.NANOSECONDS;
import static org.neo4j.values.storable.DurationFields.NANOSECONDS_OF_SECOND;
import static org.neo4j.values.storable.DurationFields.QUARTERS_OF_YEAR;
import static org.neo4j.values.storable.DurationFields.SECONDS_OF_MINUTE;
import static org.neo4j.values.storable.DurationFields.WEEKS;
import static org.neo4j.values.storable.DurationFields.YEARS;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.neo4j.values.storable.DurationFields;
import org.neo4j.values.storable.DurationValue;

public class DurationFormatUtil {
    public static String getOrCreateDurationPattern(String format) {
        final String formatLower = format.toLowerCase();
        return ISO_DURATION_PATTERNS.getOrDefault(formatLower, ELASTIC_PATTERNS.getOrDefault(formatLower, format));
    }

    public static String getDurationFormat(DurationValue duration, String formatPattern) {
        AtomicInteger idx = new AtomicInteger();
        // we split the pattern via the escape char `'` and format only odd part,
        // to manage patterns like  "yy 'years' dd 'days'"
        return Arrays.stream(formatPattern.split("'"))
                .map(item -> {
                    if ((idx.getAndIncrement() % 2) == 0) {
                        return getInnerDurationFormat(duration, item);
                    }
                    return item;
                })
                .collect(Collectors.joining());
    }

    private static String getInnerDurationFormat(DurationValue duration, String formatter) {
        return Pattern.compile(
                        "(?<year>[yYu])\\1*|(?<day>[dD])\\2*|(?<monthYear>[ML])\\3*|(?<quarterYear>[qQ])\\4*|(?<week>[wW])\\5*|"
                                + "(?<hour>[hHkK])\\6*|(?<minHour>m)\\7*|(?<secMin>s)\\8*|(?<nsSeconds>[nS])\\9*|"
                                + "(?<ms>A)\\10*|(?<ns>N)\\11|(?<iso>I)\\12*")
                .matcher(formatter)
                .replaceAll(res -> {
                    final Matcher m = (Matcher) res;
                    if (m.group("year") != null) {
                        return getFieldDigit(m, duration, YEARS);
                    }
                    if (m.group("day") != null) {
                        return getFieldDigit(m, duration, DAYS);
                    }
                    if (m.group("monthYear") != null) {
                        return getFieldDigit(m, duration, MONTHS_OF_YEAR);
                    }
                    if (m.group("quarterYear") != null) {
                        return getFieldDigit(m, duration, QUARTERS_OF_YEAR);
                    }
                    if (m.group("week") != null) {
                        return getFieldDigit(m, duration, WEEKS);
                    }
                    if (m.group("hour") != null) {
                        return getFieldDigit(m, duration, HOURS);
                    }
                    if (m.group("minHour") != null) {
                        return getFieldDigit(m, duration, MINUTES_OF_HOUR);
                    }
                    if (m.group("secMin") != null) {
                        return getFieldDigit(m, duration, SECONDS_OF_MINUTE);
                    }
                    if (m.group("nsSeconds") != null) {
                        return getFieldDigit(m, duration, NANOSECONDS_OF_SECOND, true);
                    }
                    if (m.group("ms") != null) {
                        return getFieldDigit(m, duration, MILLISECONDS);
                    }
                    if (m.group("ns") != null) {
                        return getFieldDigit(m, duration, NANOSECONDS);
                    }
                    // the letter `I` is used to create nanoseconds in iso format, i.e. with trailing zeros. e.g.
                    // '123000' become '123'
                    if (m.group("iso") != null) {
                        final String isoNanos = getFieldDigit(m, duration, NANOSECONDS_OF_SECOND)
                                .replaceAll("(?!$)0+$", "");
                        return isoNanos.isEmpty() ? "" : ("." + isoNanos);
                    }
                    // fallback
                    return formatter;
                });
    }

    private static String getFieldDigit(Matcher m, DurationValue duration, DurationFields field) {
        return getFieldDigit(m, duration, field, false);
    }

    private static String getFieldDigit(Matcher m, DurationValue duration, DurationFields field, boolean toTruncate) {
        long replacement = duration.get(field.propertyKey).value();
        // to add padding zeros, e.g. a duration of 15 years with a pattern 'yyyy' will have m.group().length() = 4
        // so the string will be String.format("%04d", years>) --> 0015
        final String formatted = String.format("%0" + m.group().length() + "d", replacement);
        return toTruncate ? formatted.substring(0, m.group().length()) : formatted;
    }
}
