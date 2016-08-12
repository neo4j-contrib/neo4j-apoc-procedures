package apoc.spatial;

import apoc.ApocConfiguration;
import apoc.util.JsonUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.net.URL;
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
        assumeTravis();
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        registerProcedure(db, Geocode.class);
    }

    @After
    public void tearDown() {
        if (db!=null) {
            db.shutdown();
        }
    }

    @Test
    public void testGeocodeOSM() throws Exception {
        testGeocodeWithThrottling("osm");
    }

    @Test
    public void testGeocodeGoogle() throws Exception {
        testGeocodeWithThrottling("google");
    }

    private void testGeocodeWithThrottling(String supplier) throws Exception {
        long fast = testGeocode(supplier, 100);
        System.out.println("Fast " + supplier + " test took " + fast + "ms");
        long slow = testGeocode(supplier, 2000);
        System.out.println("Slow " + supplier + " test took " + slow + "ms");
        assertTrue("Fast " + supplier + " took " + fast + "ms and slow took " + slow + "ms, but expected slow to be at least twice as long", (1.0 * slow / fast) > 2.0);
    }

    private long testGeocode(String provider, long throttle) throws Exception {
        setupSupplier(provider, throttle);
//        testConfig(provider);
        URL url = ClassLoader.getSystemResource("spatial.json");
        Map tests = (Map) JsonUtil.OBJECT_MAPPER.readValue(url.openConnection().getInputStream(), Object.class);
        long start = System.currentTimeMillis();
        for (Object address : (List) tests.get("addresses")) {
            testGeocodeAddress((Map) address, provider);
        }
        return System.currentTimeMillis() - start;
    }

    private void setupSupplier(String name, long throttle) {
        ApocConfiguration.addToConfig(map(
                Geocode.PREFIX + "." +Geocode.GEOCODE_PROVIDER_KEY, name,
                Geocode.PREFIX + "." + name + ".throttle", Long.toString(throttle)));
    }

    private void testGeocodeAddress(Map map, String provider) {
        if (map.containsKey("noresults")) {
            for (String field : new String[]{"address", "noresults"}) {
                assertTrue("Expected " + field + " field", map.containsKey(field));
            }
            testCallEmpty(db, "CALL apoc.spatial.geocode({url},0)", map("url", map.get("address").toString()));
        } else if (map.containsKey("count")) {
            if (((Map) map.get("count")).containsKey(provider)) {
                for (String field : new String[]{"address", "count"}) {
                    assertTrue("Expected " + field + " field", map.containsKey(field));
                }
                testCallCount(db, "CALL apoc.spatial.geocode({url},0)",
                        map("url", map.get("address").toString()),
                        (int) ((Map) map.get("count")).get(provider));
            }
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
        testCall(db, "CALL apoc.spatial.geocodeOnce({url})", map("url", address),
                (row) -> {
                    Map value = (Map) row.get("location");
                    assertEquals("Incorrect latitude found", lat, Double.parseDouble(value.get("latitude").toString()),
                            0.1);
                    assertEquals("Incorrect longitude found", lon, Double.parseDouble(value.get("longitude").toString()),
                            0.1);
                });
    }

/*
    private void testConfig(String provider) {
        testCall(db, "CALL apoc.spatial.config({config})", map("config", map(GEO_PREFIX + ".test", provider)),
                (row) -> {
                    Map<String, String> value = (Map) row.get("value");
                    assertEquals("Expected provider to be set in '" + GEO_PREFIX + ".test'", provider, value.get(GEO_PREFIX + ".test"));
                    assertEquals(provider, value.get(Geocode.GEOCODE_PROVIDER_KEY));
                    String throttleKey = GEO_PREFIX + "." + provider + ".throttle";
                    assertTrue("Expected a throttle setting", value.containsKey(throttleKey));
                    assertTrue("Expected a valid throttle setting", Long.parseLong(value.get(throttleKey)) > 0);
                });
    }
*/
}
