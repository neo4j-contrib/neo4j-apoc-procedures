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
        map.put("basic_date", DateTimeFormatter.ofPattern("yyyyMMdd"));
        map.put("basic_date_time", DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss.SSSZ"));
        map.put("basic_date_time_no_millis", DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmssZ"));
        map.put("basic_ordinal_date", DateTimeFormatter.ofPattern("yyyyDDD"));
        map.put("basic_ordinal_date_time", DateTimeFormatter.ofPattern("yyyyDDD'T'HHmmss.SSSZ"));
        map.put("basic_ordinal_date_time_no_millis", DateTimeFormatter.ofPattern("yyyyDDD'T'HHmmssZ"));
        map.put("basic_time", DateTimeFormatter.ofPattern("HHmmss.SSSZ"));
        map.put("basic_time_no_millis", DateTimeFormatter.ofPattern("HHmmssZ"));
        map.put("basic_t_time", DateTimeFormatter.ofPattern("'T'HHmmss.SSSZ"));
        map.put("basic_t_time_no_millis", DateTimeFormatter.ofPattern("'T'HHmmssZ"));
        map.put("basic_week_date", DateTimeFormatter.ofPattern("xxxx'W'wwe"));
        map.put("basic_week_date_time", DateTimeFormatter.ofPattern("xxxx'W'wwe'T'HHmmss.SSSZ"));
        map.put("basic_week_date_time_no_millis", DateTimeFormatter.ofPattern("xxxx'W'wwe'T'HHmmssZ"));
        map.put("date", DateTimeFormatter.ofPattern("yyyy-MM-dd"));
        map.put("date_hour", DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH"));
        map.put("date_hour_minute", DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm"));
        map.put("date_hour_minute_second", DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"));
        map.put("date_hour_minute_second_fraction", DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS"));
        map.put("date_hour_minute_second_millis", DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS"));
        map.put("date_time", DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZZ"));
        map.put("date_time_no_millis", DateTimeFormatter.ofPattern("yyy-MM-dd'T'HH:mm:ssZZ"));
        map.put("hour", DateTimeFormatter.ofPattern("HH"));
        map.put("hour_minute", DateTimeFormatter.ofPattern("HH:mm"));
        map.put("hour_minute_second", DateTimeFormatter.ofPattern("HH:mm:ss"));
        map.put("hour_minute_second_fraction", DateTimeFormatter.ofPattern("HH:mm:ss.SSS"));
        map.put("hour_minute_second_millis", DateTimeFormatter.ofPattern("HH:mm:ss.SSS"));
        map.put("ordinal_date", DateTimeFormatter.ofPattern("yyyy-DDD"));
        map.put("ordinal_date_time", DateTimeFormatter.ofPattern("yyyy-DDD'T'HH:mm:ss.SSSZZ"));
        map.put("ordinal_date_time_no_millis", DateTimeFormatter.ofPattern("yyyy-DDD'T'HH:mm:ssZZ"));
        map.put("time", DateTimeFormatter.ofPattern("HH:mm:ss.SSSZZ"));
        map.put("time_no_millis", DateTimeFormatter.ofPattern("HH:mm:ssZZ"));
        map.put("t_time", DateTimeFormatter.ofPattern("'T'HH:mm:ss.SSSZZ"));
        map.put("t_time_no_millis", DateTimeFormatter.ofPattern("'T'HH:mm:ssZZ"));
        map.put("week_date", DateTimeFormatter.ofPattern("xxxx-'W'ww-e"));
        map.put("week_date_time", DateTimeFormatter.ofPattern("xxxx-'W'ww-e'T'HH:mm:ss.SSSZZ"));
        map.put("week_date_time_no_millis", DateTimeFormatter.ofPattern("xxxx-'W'ww-e'T'HH:mm:ssZZ"));
        map.put("weekyear", DateTimeFormatter.ofPattern("xxxx"));
        map.put("weekyear_week", DateTimeFormatter.ofPattern("xxxx-'W'ww"));
        map.put("weekyear_week_day", DateTimeFormatter.ofPattern("xxxx-'W'ww-e"));
        map.put("year", DateTimeFormatter.ofPattern("yyyy"));
        map.put("year_month", DateTimeFormatter.ofPattern("yyyy-MM"));
        map.put("year_month_day", DateTimeFormatter.ofPattern("yyyy-MM-dd"));
        map.put( "rfc_1123_date_time", DateTimeFormatter.RFC_1123_DATE_TIME );
        ISO_DATE_FORMAT = Collections.unmodifiableMap(map);
    }

    public static DateTimeFormatter getOrCreate(String format) {
        return ISO_DATE_FORMAT.containsKey(format.toLowerCase()) ? ISO_DATE_FORMAT.get(format.toLowerCase()) : DateTimeFormatter.ofPattern(format);
    }

    public static Set<String> getTypes() {
        return ISO_DATE_FORMAT.keySet();
    }
}
