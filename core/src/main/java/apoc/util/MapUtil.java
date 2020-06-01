package apoc.util;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author mh
 * @since 04.05.16
 */
public class MapUtil {
    public static Map<String,Object> map(Object ... values) {
        return Util.map(values);
    }
}
