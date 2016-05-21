package apoc.util;

import org.junit.Test;

import static apoc.util.MapUtil.map;
import static org.junit.Assert.*;

/**
 * @author mh
 * @since 19.05.16
 */
public class UtilTest {

    @Test
    public void testSubMap() throws Exception {
        assertEquals(map("b","c"),Util.subMap(map("a.b","c"),"a"));
        assertEquals(map("b","c"),Util.subMap(map("a.b","c"),"a."));
        assertEquals(map(),Util.subMap(map("a.b","c"),"x"));
    }
}
