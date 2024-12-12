package apoc.util;

import java.util.Map;

public class ExtendedMapUtils {

    public static int size(final Map<?, ?> map) {
        return map == null ? 0 : map.size();
    }
    
    public static boolean isEmpty(final Map<?,?> map) {
        return map == null || map.isEmpty();
    }
    
    public static boolean isNotEmpty(final Map<?,?> map) {
        return !isEmpty(map);
    }
}
