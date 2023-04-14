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

import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class DateFormatUtil {

    private static final Map<String, DateTimeFormatter> ISO_DATE_FORMAT;

    static {
        Map<String, DateTimeFormatter> map = new HashMap<>();
        //Static default formatters from Java ISO and Elasticsearch patterns
        map.put("basic_iso_date", DateTimeFormatter.BASIC_ISO_DATE);
        map.put("iso_date", DateTimeFormatter.ISO_DATE);
        map.put("iso_instant", DateTimeFormatter.ISO_INSTANT);
        map.put("iso_date_time", DateTimeFormatter.ISO_DATE_TIME);
        map.put("iso_local_date", DateTimeFormatter.ISO_LOCAL_DATE);
        map.put("iso_local_date_time", DateTimeFormatter.ISO_LOCAL_DATE_TIME);
        map.put("iso_local_time", DateTimeFormatter.ISO_LOCAL_TIME);
        map.put("iso_offset_date", DateTimeFormatter.ISO_OFFSET_DATE);
        map.put("iso_offset_date_time", DateTimeFormatter.ISO_OFFSET_DATE_TIME);
        map.put("iso_offset_time", DateTimeFormatter.ISO_OFFSET_TIME);
        map.put("iso_ordinal_date", DateTimeFormatter.ISO_ORDINAL_DATE);
        map.put("iso_time", DateTimeFormatter.ISO_TIME);
        map.put("iso_week_date", DateTimeFormatter.ISO_WEEK_DATE);
        map.put("iso_zoned_date_time", DateTimeFormatter.ISO_ZONED_DATE_TIME);
        map.put( "rfc_1123_date_time", DateTimeFormatter.RFC_1123_DATE_TIME );
        ISO_DATE_FORMAT = Collections.unmodifiableMap(map);
    }

    public static Map<String, String> ISO_DURATION_PATTERNS = new HashMap<>() {{
        //Static default formatters from Java ISO, excluding those with timezone 
        put("basic_iso_date", "yyyyMMdd");
        // the letter `I` is used to create nanoseconds in iso format, i.e. with trailing zeros. e.g. '123000' become '123'
        final String isoDateTime = "yyyy-MM-dd'T'HH:mm:ssI";
        put("iso_date_time", isoDateTime);
        put("iso_local_date_time", isoDateTime);
        final String isoDate = "yyyy-MM-dd";
        put("iso_date", isoDate);
        put("iso_local_date", isoDate);
        final String isoTime = "HH:mm:ssI";
        put("iso_time", isoTime);
        put("iso_local_time", isoTime);
        put("iso_ordinal_date", "yyyy-DDD"); 
        put("iso_week_date", "YYYY-'W'ww-e");
    }};

    public static Map<String, String> ELASTIC_PATTERNS = new HashMap<>() {{
        //Static default formatters from Elasticsearch patterns
        put("basic_date", "yyyyMMdd");
        put("basic_date_time", "yyyyMMdd'T'HHmmss.SSSZ");
        put("basic_date_time_no_millis", "yyyyMMdd'T'HHmmssZ");
        put("basic_ordinal_date", "yyyyDDD");
        put("basic_ordinal_date_time", "yyyyDDD'T'HHmmss.SSSZ");
        put("basic_ordinal_date_time_no_millis", "yyyyDDD'T'HHmmssZ");
        put("basic_time", "HHmmss.SSSZ");
        put("basic_time_no_millis", "HHmmssZ");
        put("basic_t_time", "'T'HHmmss.SSSZ");
        put("basic_t_time_no_millis", "'T'HHmmssZ");
        put("basic_week_date", "xxxx'W'wwe");
        put("basic_week_date_time", "xxxx'W'wwe'T'HHmmss.SSSZ");
        put("basic_week_date_time_no_millis", "xxxx'W'wwe'T'HHmmssZ");
        put("date", "yyyy-MM-dd");
        put("date_hour", "yyyy-MM-dd'T'HH");
        put("date_hour_minute", "yyyy-MM-dd'T'HH:mm");
        put("date_hour_minute_second", "yyyy-MM-dd'T'HH:mm:ss");
        put("date_hour_minute_second_fraction", "yyyy-MM-dd'T'HH:mm:ss.SSS");
        put("date_hour_minute_second_millis", "yyyy-MM-dd'T'HH:mm:ss.SSS");
        put("date_time", "yyyy-MM-dd'T'HH:mm:ss.SSSZZ");
        put("date_time_no_millis", "yyy-MM-dd'T'HH:mm:ssZZ");
        put("hour", "HH");
        put("hour_minute", "HH:mm");
        put("hour_minute_second", "HH:mm:ss");
        put("hour_minute_second_fraction", "HH:mm:ss.SSS");
        put("hour_minute_second_millis", "HH:mm:ss.SSS");
        put("ordinal_date", "yyyy-DDD");
        put("ordinal_date_time", "yyyy-DDD'T'HH:mm:ss.SSSZZ");
        put("ordinal_date_time_no_millis", "yyyy-DDD'T'HH:mm:ssZZ");
        put("time", "HH:mm:ss.SSSZZ");
        put("time_no_millis", "HH:mm:ssZZ");
        put("t_time", "'T'HH:mm:ss.SSSZZ");
        put("t_time_no_millis", "'T'HH:mm:ssZZ");
        put("week_date", "xxxx-'W'ww-e");
        put("week_date_time", "xxxx-'W'ww-e'T'HH:mm:ss.SSSZZ");
        put("week_date_time_no_millis", "xxxx-'W'ww-e'T'HH:mm:ssZZ");
        put("weekyear", "xxxx");
        put("weekyear_week", "xxxx-'W'ww");
        put("weekyear_week_day", "xxxx-'W'ww-e");
        put("year", "yyyy");
        put("year_month", "yyyy-MM");
        put("year_month_day", "yyyy-MM-dd");
    }};

    public static DateTimeFormatter getOrCreate(String format) {
        final String formatLower = format.toLowerCase();
        
        if (ISO_DATE_FORMAT.containsKey(formatLower)) {
            return ISO_DATE_FORMAT.get(formatLower);
        }
        return DateTimeFormatter.ofPattern(ELASTIC_PATTERNS.getOrDefault(formatLower, format));
    }

    public static Set<String> getTypes() {
        return ISO_DATE_FORMAT.keySet();
    }
}
