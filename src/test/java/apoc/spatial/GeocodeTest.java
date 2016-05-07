package apoc.spatial;

import apoc.util.JsonUtil;
import apoc.util.TestUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class GeocodeTest {

    private GraphDatabaseService db;

    @Before
    public void setUp() throws Exception {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        TestUtil.registerProcedure(db, Geocode.class);
    }

    @After
    public void tearDown() {
        db.shutdown();
    }

    private void setupSupplier(String name, long throttle) {
        Config config = ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency(Config.class);
        Map<String, String> newParams = new HashMap<>();
        newParams.put("apoc.spatial.geocode.supplier", name);
        newParams.put("apoc.spatial.geocode.osm.throttle", Long.toString(throttle));
        config.augment(newParams);
    }

    @Test
    public void testGeocodeOSM() throws Exception {
        long fast = testGeocode("osm", 100);
        System.out.println("Fast OSM test took " + fast + "ms");
        long slow = testGeocode("osm", 2000);
        System.out.println("Slow OSM test took " + slow + "ms");
        assertTrue("Fast OSM took " + fast + "ms and slow took " + slow + "ms, but expected slow to be at least twice as long", (1.0 * slow / fast) > 2.0);
    }

    @Test
    public void testGeocodeGoogle() throws Exception {
        long duration = testGeocode("google", 0);
        System.out.println("Google test took " + duration + "ms");
    }

    private long testGeocode(String provider, long throttle) throws Exception {
        setupSupplier(provider, throttle);
        URL url = ClassLoader.getSystemResource("spatial.json");
        Map tests = (Map) JsonUtil.OBJECT_MAPPER.readValue(url.openConnection().getInputStream(), Object.class);
        long start = System.currentTimeMillis();
        for (Object address : (List) tests.get("addresses")) {
            testGeocodeAddress((Map) address, provider);
        }
        return System.currentTimeMillis() - start;
    }

    private void testGeocodeAddress(Map map, String provider) {
        if (map.containsKey("noresults")) {
            for (String field : new String[]{"address", "noresults"}) {
                assertTrue("Expected " + field + " field", map.containsKey(field));
            }
            testCallEmpty(db, "CALL apoc.spatial.geocode({url},0)", map("url", map.get("address").toString()));
        } else if (map.containsKey("count")) {
            for (String field : new String[]{"address", "count"}) {
                assertTrue("Expected " + field + " field", map.containsKey(field));
            }
            testCallCount(db, "CALL apoc.spatial.geocode({url},0)",
                    map("url", map.get("address").toString()),
                    (int) ((Map) map.get("count")).get(provider));
        } else {
            for (String field : new String[]{"address", "latitude", "longitude"}) {
                assertTrue("Expected " + field + " field", map.containsKey(field));
            }
            testGeocodeAddress(map.get("address").toString(),
                    (double) map.get("latitude"),
                    (double) map.get("longitude"));
        }
    }

    private void testGeocodeAddress(String address, double lat, double lon) {
        testCall(db, "CALL apoc.spatial.geocode({url},0)", map("url", address),
                (row) -> {
                    Map value = (Map) row.get("location");
                    assertEquals("Incorrect latitude found", lat, Double.parseDouble(value.get("latitude").toString()),
                            0.0005);
                    assertEquals("Incorrect longitude found", lon, Double.parseDouble(value.get("longitude").toString()),
                            0.0005);
                });
    }
}
