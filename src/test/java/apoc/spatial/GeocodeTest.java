package apoc.spatial;

import apoc.ApocConfiguration;
import apoc.util.JsonUtil;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.io.InputStream;
import java.util.List;
import java.util.Map;

import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.assumeTravis;
import static apoc.util.TestUtil.registerProcedure;
import static apoc.util.TestUtil.testCallCount;
import static apoc.util.TestUtil.testCallEmpty;
import static apoc.util.TestUtil.testResult;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class GeocodeTest {

    private GraphDatabaseService db;

    @Before
    public void setUp() throws Exception {
        assumeTravis();
        // Impermanent Database creation with additional configuration
        db = new TestGraphDatabaseFactory()
                .newImpermanentDatabaseBuilder()
                .loadPropertiesFromFile("src/test/resources/geoSpatial.conf")
                .newGraphDatabase();
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
        testGeocodeWithThrottling("osm", false);
    }

    @Test
    public void testReverseGeocodeOSM() throws Exception {
        testGeocodeWithThrottling("osm", true);
    }

    @Ignore
    @Test
    public void testGeocodeGoogle() throws Exception {
        testGeocodeWithThrottling("google", false);
    }

    @Test
    public void testReverseGeocodeGoogle() throws Exception {
        testGeocodeWithThrottling("google", true);
    }

    @Test
    public void testGeocodeOpenCage() throws Exception {
        // If the key is not defined the test won't fail
        String provider = ApocConfiguration.get(Geocode.PREFIX).get(Geocode.GEOCODE_PROVIDER_KEY).toString().toLowerCase();
        Assume.assumeTrue(!"<YOUR_API_KEY>".equals(ApocConfiguration.get(Geocode.PREFIX).get(provider + ".key").toString()));

        // We use testGeocode() instead of testGeocodeWithThrottling() because the slow test takes less time than the fast one
        // The overall execution is strictly tight to the remote service according to quota and request policies
        testGeocode("openCage",1000, false);
    }

    @Test
    public void testReverseGeocodeOpenCage() throws Exception {
        // If the key is not defined the test won't fail
        String provider = ApocConfiguration.get(Geocode.PREFIX).get(Geocode.GEOCODE_PROVIDER_KEY).toString().toLowerCase();
        Assume.assumeTrue(!"<YOUR_API_KEY>".equals(ApocConfiguration.get(Geocode.PREFIX).get(provider + ".key").toString()));

        testGeocode("openCage",1000, true);
    }

    private void testGeocodeWithThrottling(String supplier, Boolean reverseGeocode) throws Exception {
        long fast = testGeocode(supplier, 100, reverseGeocode);
        long slow = testGeocode(supplier, 2000, reverseGeocode);
        assertTrue("Fast " + supplier + " took " + fast + "ms and slow took " + slow + "ms, but expected slow to be at least twice as long", (1.0 * slow / fast) > 1.2);
    }

    private long testGeocode(String provider, long throttle, boolean reverseGeocode) throws Exception {
        setupSupplier(provider, throttle);
//        testConfig(provider);
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
        try {
            testResult(db,"CALL apoc.spatial.reverseGeocode({latitude},{longitude})",map("latitude", latitude, "longitude", longitude), (row)->{
                row.forEachRemaining((r)->{
                    assertNotNull(r.get("description"));
                    assertNotNull(r.get("location"));
                    assertNotNull(r.get("data"));
                });
            });
        } catch(Exception e) {
            Assume.assumeNoException("out of quota", e);
        }
    }


    private void setupSupplier(String name, long throttle) {
        ApocConfiguration.addToConfig(map(
                Geocode.PREFIX + "." +Geocode.GEOCODE_PROVIDER_KEY, name,
                Geocode.PREFIX + "." + name + ".throttle", Long.toString(throttle)));
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
            testCallEmpty(db, "CALL apoc.spatial.geocode({url},0)", map("url", map.get("address").toString()));
        } else if (map.containsKey("count")) {
            if (((Map) map.get("count")).containsKey(provider)) {
                for (String field : new String[]{"address", "count"}) {
                    assertTrue("Expected " + field + " field", map.containsKey(field));
                }
                testCallCount(db, "CALL apoc.spatial.geocode({url},0)",
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
        testResult(db, "CALL apoc.spatial.geocodeOnce({url})", map("url", address),
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
