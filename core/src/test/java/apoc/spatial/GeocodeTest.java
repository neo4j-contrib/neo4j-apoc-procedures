package apoc.spatial;

import apoc.util.JsonUtil;
import apoc.util.TestUtil;
import inet.ipaddr.IPAddressString;
import org.junit.*;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.graphdb.Result;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static apoc.ApocConfig.apocConfig;
import static apoc.util.MapUtil.map;
import static org.junit.Assert.*;
import static org.neo4j.test.assertion.Assert.assertEventually;

public class GeocodeTest {

    private static final String BLOCKED_ADDRESS = "127.168.0.0";
    private static final String NON_BLOCKED_ADDRESS = "localhost";
    private static final String BLOCKED_ERROR = "access to /" + BLOCKED_ADDRESS + " is blocked via the configuration property unsupported.dbms.cypher_ip_blocklist";
    private static final String JAVA_NET_EXCEPTION = "Caused by: java.net";
    private static final String URL_FORMAT = "%s://%s/geocode/v1/json?q=PLACE&key=KEY";
    private static final String REVERSE_URL_FORMAT = "%s://%s/geocode/v1/json?q=LAT+LNG&key=KEY";

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule()
            .withSetting( GraphDatabaseInternalSettings.cypher_ip_blocklist, List.of(new IPAddressString(BLOCKED_ADDRESS)) );

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
    public void testGeocodeWithBlockedAddressWithApocConf() {
        final String geocodeBaseConfig = Geocode.PREFIX + ".opencage";
        apocConfig().setProperty(geocodeBaseConfig + ".key", "myKey");
        
        Stream.of("https", "http", "ftp").forEach(protocol -> {
            final String nonBlockedUrl = String.format(URL_FORMAT, protocol, NON_BLOCKED_ADDRESS);
            final String nonBlockedReverseUrl = String.format(REVERSE_URL_FORMAT, protocol, NON_BLOCKED_ADDRESS);
            
            final String geocodeConfigUrl = geocodeBaseConfig + ".url";
            final String geocodeConfigReverseUrl = geocodeBaseConfig + ".reverse.url";
            
            apocConfig().setProperty(geocodeConfigUrl, nonBlockedUrl);
            apocConfig().setProperty(geocodeConfigReverseUrl, String.format(REVERSE_URL_FORMAT, protocol, BLOCKED_ADDRESS));

            assertGeocodeBlockedUrl(true);
            
            apocConfig().setProperty(geocodeConfigUrl, String.format(URL_FORMAT, protocol, BLOCKED_ADDRESS));
            apocConfig().setProperty(geocodeConfigReverseUrl, nonBlockedReverseUrl);

            assertGeocodeBlockedUrl(false);


            apocConfig().setProperty(geocodeConfigUrl, nonBlockedUrl);
            apocConfig().setProperty(geocodeConfigReverseUrl, nonBlockedReverseUrl);
            
            assertGeocodeAllowedUrl(false);
            assertGeocodeAllowedUrl(true);
        });
    }
    
    @Test
    public void testGeocodeWithBlockedAddressWithConfigMap() {
        Stream.of("https", "http", "ftp").forEach(protocol -> {

            final String nonBlockedUrl = String.format(URL_FORMAT, protocol, NON_BLOCKED_ADDRESS);
            final String nonBlockedReverseUrl = String.format(REVERSE_URL_FORMAT, protocol, NON_BLOCKED_ADDRESS);

            assertGeocodeBlockedUrl(true, 
                    nonBlockedUrl,
                    String.format(REVERSE_URL_FORMAT, protocol, BLOCKED_ADDRESS)
            );

            assertGeocodeBlockedUrl(false, 
                    String.format(URL_FORMAT, protocol, BLOCKED_ADDRESS),
                    nonBlockedReverseUrl
            );

            assertGeocodeAllowedUrl(false,
                    nonBlockedUrl, nonBlockedReverseUrl);

            assertGeocodeAllowedUrl(true,
                    nonBlockedUrl, nonBlockedReverseUrl);
        });
    }

    private void assertGeocodeBlockedUrl(boolean reverseGeocode) {
        assertGeocodeBlockedUrl(reverseGeocode, null, null);
    }
    
    private void assertGeocodeBlockedUrl(boolean reverseGeocode, String url, String reverseUrl) {
        // check that if either url or reverse address are blocked 
        // respectively the apoc.spatial.geocode and the apoc.spatial.reverseGeocode procedure fails
        assertGeocodeFails(reverseGeocode, BLOCKED_ERROR, url, reverseUrl);
    }

    private void assertGeocodeAllowedUrl(boolean reverseGeocode) {
        assertGeocodeAllowedUrl(reverseGeocode, null, null);
    }

    private void assertGeocodeAllowedUrl(boolean reverseGeocode, String url, String reverseUrl) {
        // check that if neither url nor reverse url are blocked 
        // the procedures continue the execution (in this case by throwing a `401` Exception)
        assertGeocodeFails(reverseGeocode, JAVA_NET_EXCEPTION, url, reverseUrl);
    }

    private void assertGeocodeFails(boolean reverseGeocode, String expectedMsgError, String url, String reverseUrl) {
        // url == null means that it is defined via apoc.conf
        Map<String, Object> conf = url == null
                ? Collections.emptyMap()
                : Map.of("key", "myOwnKey", 
                    "url", url, 
                    "reverseUrl", reverseUrl);
        
        assertGeocodeFails(reverseGeocode, expectedMsgError, conf);
    }

    private void assertGeocodeFails(boolean reverseGeocode, String expectedMsgError, Map<String, Object> conf) {
        QueryExecutionException e = assertThrows(QueryExecutionException.class,
                () -> testGeocode( "opencage", 100, reverseGeocode, conf )
        );

        final String actualMsgErr = e.getMessage();
        assertTrue("Actual err. message is " + actualMsgErr, actualMsgErr.contains(expectedMsgError));
    }

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
        AtomicLong time = new AtomicLong();

        if(reverseGeocode) {
            for(Object address : (List) tests.get("events")) {
                testReverseGeocodeAddress(((Map)address).get("lat"), ((Map)address).get("lon"), time, config);
            }
        } else {
            for (Object address : (List) tests.get("addresses")) {
                testGeocodeAddress((Map) address, (String) config.getOrDefault("provider", provider), time, config);
            }
        }

        return time.get();
    }

    private void testReverseGeocodeAddress(Object latitude, Object longitude, AtomicLong time, Map<String, Object> config) {
        ignoreQuotaError(() -> {
            String query = "CALL apoc.spatial.reverseGeocode($latitude, $longitude, false, $config)";
            Map<String, Object> params = Map.of("latitude", latitude, "longitude", longitude, "config", config);
            waitForServerResponseOK(query, params, time,
                    (res) -> {
                        assertTrue(res.hasNext());
                        res.forEachRemaining((r) -> {
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

    private void testGeocodeAddress(Map map, String provider, AtomicLong time, Map<String, Object> config) {
        ignoreQuotaError(() -> {
            String query = "CALL apoc.spatial.geocode('FRANCE',1,true,$config)";
            Map<String, Object> params = Map.of("config", config);
            waitForServerResponseOK(query, params, time,
                    (res) -> {
                        res.forEachRemaining((r) -> {
                            assertNotNull(r.get("description"));
                            assertNotNull(r.get("location"));
                            assertNotNull(r.get("data"));
                        });
                    });
        });
        String geocodeQuery = "CALL apoc.spatial.geocode($url,0)";
        if (map.containsKey("noresults")) {
            for (String field : new String[]{"address", "noresults"}) {
                checkJsonFields(map, field);
            }
            waitForServerResponseOK(geocodeQuery,
                    map("url", map.get("address").toString()),
                    time,
                    (res) -> assertFalse(res.hasNext())
            );
        } else if (map.containsKey("count")) {
            if (((Map) map.get("count")).containsKey(provider)) {
                for (String field : new String[]{"address", "count"}) {
                    checkJsonFields(map, field);
                }
                waitForServerResponseOK(geocodeQuery,
                        map("url", map.get("address").toString()),
                        time,
                        (res) -> {
                            long actual = Iterators.count(res);
                            int expected = ((Number) ((Map) map.get("count")).get(provider)).intValue();
                            assertEquals(expected, actual);
                        });
            }
        } else {
            for (String field : new String[]{"address", "osm"}) {
                checkJsonFields(map, field);
            }
            testGeocodeAddress(map.get("address").toString(),
                    getCoord(map, provider, "latitude"),
                    getCoord(map, provider, "longitude"),
                    time,
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

    private void testGeocodeAddress(String address, double lat, double lon, AtomicLong time, Map<String, Object> config) {
        String query = "CALL apoc.spatial.geocodeOnce($url, $config)";
        Map<String, Object> params = Map.of("url", address, "config", config);
        waitForServerResponseOK(query, params, time,
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

    private void waitForServerResponseOK(String query, Map<String, Object> params, AtomicLong time, Consumer<Result> resultObjectFunction) {
        assertEventually(() -> {
            try {
                long start = System.currentTimeMillis();
                db.executeTransactionally(query,
                        params,
                        res -> {
                            resultObjectFunction.accept(res);
                            return null;
                        });

                time.addAndGet( System.currentTimeMillis() - start );
                return true;
            } catch (Exception e) {
                if (e.getMessage().contains("Server returned HTTP response code")) {
                    return false;
                }
                throw e;
            }
        }, (value) -> value, 20L, TimeUnit.SECONDS);
    }
}
