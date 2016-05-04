package apoc.util;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author mh
 * @since 04.05.16
 */
public class MapUtil {
    public static Map<String,Object> map(Object ... values) {
        Map<String, Object> map = new LinkedHashMap<>();
        for (int i = 0; i < values.length; i+=2) {
            map.put(values[i].toString(),values[i+1]);
        }
        return map;
    }
}
