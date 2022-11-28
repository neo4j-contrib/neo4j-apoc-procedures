package apoc.spatial;

import apoc.util.JsonUtil;
import apoc.util.TestUtil;
import org.junit.*;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.io.InputStream;
import java.util.List;
import java.util.Map;

import static apoc.ApocConfig.apocConfig;
import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.*;
import static org.junit.Assert.*;

public class GeocodeTest {

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();

    @Before
    public void initDb() throws Exception {
        TestUtil.registerProcedure(db, Geocode.class);
    }
    
    // -- with apoc config
    @Test
    public void testGeocodeOSM() throws Exception {
        testGeocodeWithThrottling("osm", false);
    }

    @Test
    public void testReverseGeocodeOSM() throws Exception {
        testGeocodeWithThrottling("osm", true);
    }

    private void testGeocodeWithThrottling(String supplier, Boolean reverseGeocode) throws Exception {
        long fast = testGeocode(supplier, 100, reverseGeocode);
        long slow = testGeocode(supplier, 2000, reverseGeocode);
        assertTrue("Fast " + supplier + " took " + fast + "ms and slow took " + slow + "ms, but expected slow to be at least twice as long", (1.0 * slow / fast) > 1.2);
    }

    private long testGeocode(String provider, long throttle, boolean reverseGeocode) throws Exception {
        setupSupplier(provider, throttle);
        InputStream is = getClass().getResourceAsStream("/spatial.json");
        Map tests = JsonUtil.OBJECT_MAPPER.readValue(is, Map.class);
        long start = System.currentTimeMillis();

        if(reverseGeocode) {
            for(Object address : (List) tests.get("events")) {
                testReverseGeocodeAddress(((Map)address).get("lat"), ((Map)address).get("lon"));
            }
        } else {
            for (Object address : (List) tests.get("addresses")) {
                testGeocodeAddress((Map) address, provider);
            }
        }

        return System.currentTimeMillis() - start;
    }

    private void testReverseGeocodeAddress(Object latitude, Object longitude) {
        ignoreQuotaError(() -> {
            testResult(db, "CALL apoc.spatial.reverseGeocode($latitude, $longitude, false)",
                    map("latitude", latitude, "longitude", longitude), (row) -> {
                        assertTrue(row.hasNext());
                        row.forEachRemaining((r)->{
                            assertNotNull(r.get("description"));
                            assertNotNull(r.get("location"));
                            assertNotNull(r.get("data"));
                        });
                    });
        });
    }
    
    private void ignoreQuotaError(Runnable runnable) {
        try {
            runnable.run();
        } catch(Exception e) {
            Assume.assumeNoException("out of quota", e);
        }
    }


    private void setupSupplier(String providerName, long throttle) {
        apocConfig().setProperty(Geocode.PREFIX + ".provider", providerName);
        apocConfig().setProperty(Geocode.PREFIX + "." + providerName + ".throttle", Long.toString(throttle));
    }

    private void testGeocodeAddress(Map map, String provider) {
        try {
            testResult(db,"CALL apoc.spatial.geocode('FRANCE',1,true)",(row)->{
                row.forEachRemaining((r)->{
                    assertNotNull(r.get("description"));
                    assertNotNull(r.get("location"));
                    assertNotNull(r.get("data"));
                });
            });
        } catch(Exception e) {
            Assume.assumeNoException("out of quota", e);
        }
        if (map.containsKey("noresults")) {
            for (String field : new String[]{"address", "noresults"}) {
                assertTrue("Expected " + field + " field", map.containsKey(field));
            }
            System.out.println("map = " + map);
            testCallEmpty(db, "CALL apoc.spatial.geocode($url,0)", map("url", map.get("address").toString()));
        } else if (map.containsKey("count")) {
            if (((Map) map.get("count")).containsKey(provider)) {
                for (String field : new String[]{"address", "count"}) {
                    assertTrue("Expected " + field + " field", map.containsKey(field));
                }
                testCallCount(db, "CALL apoc.spatial.geocode($url,0)",
                        map("url", map.get("address").toString()),
                        ((Number) ((Map) map.get("count")).get(provider)).intValue());
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
        testResult(db, "CALL apoc.spatial.geocodeOnce($url)", map("url", address),
                (result) -> {
                    if (result.hasNext()) {
                        Map<String, Object> row = result.next();
                        Map value = (Map) row.get("location");
                        assertNotNull("location found", value);
                        assertEquals("Incorrect latitude found", lat, Double.parseDouble(value.get("latitude").toString()),
                                0.1);
                        assertEquals("Incorrect longitude found", lon, Double.parseDouble(value.get("longitude").toString()),
                                0.1);
                        assertFalse(result.hasNext());
                    } else {
                        // over request limit
                    }
                });
    }
}
