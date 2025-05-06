package apoc.map;

import apoc.util.TestUtil;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class MapsExtendedTest {

    private static final String OLD_KEY = "depends_on";
    private static final String NEW_KEY = "new_name";
    
    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();
    
    @Before
    public void setUp() {
        TestUtil.registerProcedure(db, MapsExtended.class);
    }

    @Test
    public void testRenameKeyNotExistent() {
        Map<String, Object> map = Map.of(
                "testKey", List.of(Map.of("testKey", "some_value")),
                "name", "Mario"
        );
        TestUtil.testCall(db, "RETURN apoc.map.renameKey($map, $keyFrom, $keyTo) as value",
                Map.of("map", map, "keyFrom", "notExistent", "keyTo", "something"),
                (r) -> assertEquals(map, r.get("value")));
    }
    
    @Test
    public void testRenameKeyInnerKey() {
        Map<String, Object> map = Map.of(
                "testKey", List.of(Map.of("innerKey", "some_value")),
                "name", "Mario"
        );
        TestUtil.testCall(db, "RETURN apoc.map.renameKey($map, $keyFrom, $keyTo) as value",
                Map.of("map", map, "keyFrom", "innerKey", "keyTo", "otherKey"),
                (r) -> {
                    Map<String, Object> expected = Map.of(
                            "testKey", List.of(Map.of("otherKey", "some_value")),
                            "name", "Mario"
                    );;
                    assertEquals(expected, r.get("value"));
                });
    }
    
    @Test
    public void testRenameKey() {
        Map<String, Object> map = Map.of(
                OLD_KEY, List.of(Map.of(OLD_KEY, "some_value")),
                "name", "Mario"
        );;
        TestUtil.testCall(db, "RETURN apoc.map.renameKey($map, $keyFrom, $keyTo) as value",
                Map.of("map", map, "keyFrom", OLD_KEY, "keyTo", NEW_KEY),
                (r) -> {
                    Map<Object, Object> expected = Map.of(NEW_KEY, List.of(Map.of(NEW_KEY, "some_value")),
                            "name", "Mario"
                    );
                    Map<String, Map<String, Object>> actual = (Map) r.get("value");
                    assertEquals(expected, actual);
                });
    }

    @Test
    public void testRenameKeyComplexMap() {
        Map<String, Object> map = Map.of(
                OLD_KEY, List.of(1L, "test", Map.of(OLD_KEY, "some_value")),
                "otherKey", List.of(1L, List.of("test", Map.of(OLD_KEY, "some_value"))),
                "name", Map.of(OLD_KEY, "some_value"),
                "other", "key"
        );
        TestUtil.testCall(db, "RETURN apoc.map.renameKey($map, $keyFrom, $keyTo) as value",
                Map.of("map", map, "keyFrom", OLD_KEY, "keyTo", NEW_KEY),
                (r) -> {
                    Map<Object, Object> expected = Map.of(
                            NEW_KEY, List.of(1L, "test", Map.of(NEW_KEY, "some_value")),
                            "otherKey", List.of(1L, List.of("test", Map.of(NEW_KEY, "some_value"))),
                            "name", Map.of(NEW_KEY, "some_value"),
                            "other", "key"
                    );
                    Map<String, Map<String, Object>> actual = (Map) r.get("value");
                    assertEquals(expected, actual);
                });
    }

    @Test
    public void testRenameKeyRecursiveFalse() {
        Map<String, Object> map = Map.of(
                OLD_KEY, List.of(Map.of(OLD_KEY, "some_value")),
                "name", "Mario"
        );;
        TestUtil.testCall(db, "RETURN apoc.map.renameKey($map, $keyFrom, $keyTo, {recursive: false}) as value",
                Map.of("map", map, "keyFrom", OLD_KEY, "keyTo", NEW_KEY),
                (r) -> {
                    Map<String, Map<String, Object>> actual = (Map) r.get("value");
                    Map<Object, Object> expected = Map.of(NEW_KEY, List.of(Map.of(OLD_KEY, "some_value")),
                            "name", "Mario"
                    );
                    assertEquals(expected, actual);
                });
    }
}
