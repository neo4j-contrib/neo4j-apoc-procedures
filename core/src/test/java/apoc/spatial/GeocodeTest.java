package apoc.spatial;

import apoc.util.JsonUtil;
import apoc.util.TestUtil;
import org.junit.*;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

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

    // -- with config map
    @Test
    public void testWrongUrlButViaOtherProvider() throws Exception {
        // wrong url but doesn't fail because provider is osm, not opencage
        testGeocodeWithThrottling("osm", false, 
                map("url", "https://api.opencagedata.com/geocode/v1/json?q=PLACE&key=KEY111"));
    }
    
    @Test(expected = QueryExecutionException.class)
    public void testWrongUrlWithOpenCage() throws Exception {
        // overwrite ApocConfig provider
        testGeocodeWithThrottling("osm", false, 
                map("provider", "opencage", "url", "https://api.opencagedata.com/geocode/v1/json?q=PLACE&key=KEY111"));
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
        testGeocodeWithThrottling(supplier, reverseGeocode, Collections.emptyMap());
    }
    
    private void testGeocodeWithThrottling(String supplier, Boolean reverseGeocode, Map<String, Object> config) throws Exception {
        long fast = testGeocode(supplier, 100, reverseGeocode, config);
        long slow = testGeocode(supplier, 2000, reverseGeocode, config);
        assertTrue("Fast " + supplier + " took " + fast + "ms and slow took " + slow + "ms, but expected slow to be at least twice as long", (1.0 * slow / fast) > 1.2);
    }

    private long testGeocode(String provider, long throttle, boolean reverseGeocode, Map<String, Object> config) throws Exception {
        setupSupplier(provider, throttle);
        InputStream is = getClass().getResourceAsStream("/spatial.json");
        Map tests = JsonUtil.OBJECT_MAPPER.readValue(is, Map.class);
        long start = System.currentTimeMillis();

        if(reverseGeocode) {
            for(Object address : (List) tests.get("events")) {
                testReverseGeocodeAddress(((Map)address).get("lat"), ((Map)address).get("lon"), config);
            }
        } else {
            for (Object address : (List) tests.get("addresses")) {
                testGeocodeAddress((Map) address, (String) config.getOrDefault("provider", provider), config);
            }
        }

        return System.currentTimeMillis() - start;
    }

    private void testReverseGeocodeAddress(Object latitude, Object longitude, Map<String, Object> config) {
        ignoreQuotaError(() -> {
            testResult(db, "CALL apoc.spatial.reverseGeocode($latitude, $longitude, false, $config)",
                    map("latitude", latitude, "longitude", longitude, "config", config), (row) -> {
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
            final String message = e.getMessage().toLowerCase();
            final boolean isQuotaError = Stream.of("request entity too large", "too many requests", "quota exceeded")
                    .anyMatch(message::contains);
            if (isQuotaError) {
                Assume.assumeNoException("out of quota", e);
            }
            throw e;
        }
    }


    private void setupSupplier(String providerName, long throttle) {
        apocConfig().setProperty(Geocode.PREFIX + ".provider", providerName);
        apocConfig().setProperty(Geocode.PREFIX + "." + providerName + ".throttle", Long.toString(throttle));
    }

    private void testGeocodeAddress(Map map, String provider, Map<String, Object> config) {
        ignoreQuotaError(() -> {
            testResult(db,"CALL apoc.spatial.geocode('FRANCE',1,true,$config)",
                    map("config", config), (row)->{
                        row.forEachRemaining((r)->{
                            assertNotNull(r.get("description"));
                            assertNotNull(r.get("location"));
                            assertNotNull(r.get("data"));
                        });
                    });
        });
        if (map.containsKey("noresults")) {
            for (String field : new String[]{"address", "noresults"}) {
                checkJsonFields(map, field);
            }
            System.out.println("map = " + map);
            testCallEmpty(db, "CALL apoc.spatial.geocode($url,0)", map("url", map.get("address").toString()));
        } else if (map.containsKey("count")) {
            if (((Map) map.get("count")).containsKey(provider)) {
                for (String field : new String[]{"address", "count"}) {
                    checkJsonFields(map, field);
                }
                testCallCount(db, "CALL apoc.spatial.geocode($url,0)",
                        map("url", map.get("address").toString()),
                        ((Number) ((Map) map.get("count")).get(provider)).intValue());
            }
        } else {
            for (String field : new String[]{"address", "osm"}) {
                checkJsonFields(map, field);
            }
            testGeocodeAddress(map.get("address").toString(),
                    getCoord(map, provider, "latitude"),
                    getCoord(map, provider, "longitude"), 
                    config);
        }
    }

    private double getCoord(Map<String, Map<String, Double>> map, String provider, String coord) {
        final Map<String, Double> providerKey = map.getOrDefault(provider.toLowerCase(), map.get("osm"));
        checkJsonFields(providerKey, coord);
        return providerKey.get(coord);
    }

    private void checkJsonFields(Map map, String field) {
        assertTrue("Expected " + field + " field", map.containsKey(field));
    }

    private void testGeocodeAddress(String address, double lat, double lon, Map<String, Object> config) {
        testResult(db, "CALL apoc.spatial.geocodeOnce($url, $config)", 
                map("url", address, "config", config),
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
