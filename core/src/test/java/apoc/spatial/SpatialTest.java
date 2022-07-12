package apoc.spatial;

import apoc.ApocSettings;
import apoc.date.Date;
import apoc.util.JsonUtil;
import apoc.util.TestUtil;
import apoc.util.Util;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.net.URL;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static apoc.ApocConfig.apocConfig;
import static apoc.spatial.Geocode.getSupplierEntry;
import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.*;
import static java.util.Collections.emptyMap;
import static org.junit.Assert.*;

public class SpatialTest {

    private static final String WRONG_PROVIDER_ERR = "wrong provider";
    private static final String URL = "http://api.opencagedata.com/geocode/v1/json?q=PLACE&key=KEY";
    private static final String REVERSE_URL = "http://api.opencagedata.com/geocode/v1/json?q=LAT+LNG&key=KEY";
    @Rule
    public DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(ApocSettings.apoc_import_file_enabled, true);

    private Map<String, Map<String, Object>> eventNodes = new LinkedHashMap<>();
    private Map<String, Map<String, Object>> spaceNodes = new LinkedHashMap<>();
    private Map<String, Map<String, Object>> spaceTimeNodes = new LinkedHashMap<>();

    public static class MockGeocode {
        public static Map<String, Map> geocodeResults = null;
        public static Map<String, Map> reverseGeocodeResults = null;

        public MockGeocode() {
        }

        @Procedure("apoc.spatial.geocodeOnce")
        public Stream<Geocode.GeoCodeResult> geocodeOnce(@Name("location") String address, @Name(value="config", defaultValue = "{}") Map<String, Object> config) {
            return geocode(address, 1, false, config);
        }

        @Procedure("apoc.spatial.geocode")
        public Stream<Geocode.GeoCodeResult> geocode(@Name("location") String address, @Name("maxResults") long maxResults, @Name(value = "quotaException",defaultValue = "false") boolean quotaException, @Name(value="config", defaultValue = "{}") Map<String, Object> config) {

            if (address == null || address.isEmpty())
                return Stream.empty();
            else {
                if (geocodeResults != null && geocodeResults.containsKey(address)) {
                    // mocked GeocodeSupplier.geocode(...)
                    Map data = getGeocodeData(config, address, geocodeResults);
                    return Stream.of(new Geocode.GeoCodeResult(Util.toDouble(data.get("lat")), Util.toDouble(data.get("lon")), (String) data.get("display_name"), data));
                } else {
                    return Stream.empty();
                }
            }
        }

        @Procedure("apoc.spatial.reverseGeocode")
        public Stream<Geocode.GeoCodeResult> reverseGeocode(@Name("latitude") double latitude, @Name("longitude") double longitude, @Name(value = "quotaException", defaultValue = "false") boolean quotaException, @Name(value="config", defaultValue = "{}") Map<String, Object> config) {
            String key = latitude + "," + longitude;
            if (reverseGeocodeResults != null && reverseGeocodeResults.containsKey(key)) {
                // mocked GeocodeSupplier.reverseGeocode(...)
                Map data = getGeocodeData(config, key, reverseGeocodeResults);
                return Stream.of(new Geocode.GeoCodeResult(latitude, longitude, (String) data.get("display_name"), data));
            } else {
                return Stream.empty();
            }
        }

        private Map getGeocodeData(Map<String, Object> config, String key, Map<String, Map> geocodeResults) {
            // we get the supplier name
            final String supplier = getSupplier(config);
            // from here we mock GeocodeSupplier.reverseGeocode/geocode(...) 
            final Map<String, Map> geocodeResult = geocodeResults.get(key);
            // we get mock data by supplier, currently "osm", "opencage" or "google"
            Map data = geocodeResult.get(supplier);
            // this condition serves to ensure the implementation works correctly
            if (data == null) {
                throw new RuntimeException(WRONG_PROVIDER_ERR);
            }
            return data;
        }

        private String getSupplier(Map<String, Object> config) {
            // to make sure that the config is correctly formatted we call the correct GeocodeSupplier constructor
            final AbstractMap.SimpleEntry<Geocode.GeocodeSupplier, String> entry = getSupplierEntry(() -> {}, config);
            return entry.getValue();
        }
    
    }

    @Before
    public void setUp() throws Exception {
        TestUtil.registerProcedure(db, Date.class);
        TestUtil.registerProcedure(db, MockGeocode.class);
        URL url = ClassLoader.getSystemResource("spatial.json");
        Map tests = (Map) JsonUtil.loadJson(url.toString()).findFirst().orElse(null);
        for (Object event : (List) tests.get("events")) {
            addEventData((Map) event);
        }
        MockGeocode.geocodeResults = (Map<String, Map>) tests.get("geocode");
        MockGeocode.reverseGeocodeResults = (Map<String, Map>) tests.get("reverseGeocode");
    }

    private void addEventData(Map<String, Object> event) {
        Map<String, Object> params = map("params", event);
        int created = db.executeTransactionally("CREATE (e:Event $params)", params,
                result -> result.getQueryStatistics().getNodesCreated());
        assertEquals("Expected a node to be created", 1, created);
        String name = event.get("name").toString();
        if (!event.containsKey("toofar")) {
            spaceNodes.put(name, event);
            if (!event.containsKey("toosoon") && !event.containsKey("toolate")) {
                spaceTimeNodes.put(name, event);
            }
        }
        eventNodes.put(name, event);
    }

    @Test
    public void testSimpleGeocode() {
        final Map<String, Object> config = Collections.emptyMap();
        geocodeOnceCommon(config);
    }    
    
    @Test(expected = RuntimeException.class)
    public void testGeocodeOpencageWrongUrlFormat() {
        // with provider different from osm/google we have to explicit an url correctly formatted (i.e. with 'PLACE' string)
        try {
            final Map<String, Object> conf = map("provider", "opencage",
                    "url", "wrongUrl",
                    "reverseUrl", REVERSE_URL);
            geocodeOnceCommon(conf);
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains("Missing 'PLACE' in url template"));
            throw e;
        }
    }

    @Test(expected = RuntimeException.class)
    public void testGeocodeOpencageMissingKey() {
        // with provider (via config map) different from osm/google we have to explicit the key
        try {
            final Map<String, Object> conf = map("provider", "opencage",
                    "url", URL, "reverseUrl", REVERSE_URL);
            geocodeOnceCommon(conf);
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains("Missing 'key' for geocode provider"));
            throw e;
        }
    }

    @Test(expected = RuntimeException.class)
    public void testGeocodeOpencageMissingKeyViaApocConfig() {
        // with provider(via apocConfig()) different from osm/google we have to explicit the key
        apocConfig().setProperty(Geocode.PREFIX + ".provider", "something");
        try {
            final Map<String, Object> conf = map("url", URL, "reverseUrl", REVERSE_URL);
            geocodeOnceCommon(conf);
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains("Missing 'key' for geocode provider"));
            throw e;
        }
    }

    @Test
    public void testSimpleGeocodeViaApocConfig() {
        // Missing key but doesn't fail because provider is google via ApocConfig, not opencage like testGeocodeOpencageMissingKey
        apocConfig().setProperty(Geocode.PREFIX + ".provider", "google");
        final Map<String, Object> config = map("url", "mockUrl", "reverseUrl", "mockReverseUrl");
        geocodeOnceCommon(config);
    }

    @Test
    public void testSimpleGeocodeOpencageOverwiteApocConfigs() {
        // the key is defined in apocConfig()
        // the url and provider are in both apocConfig() and config map, but the second ones win
        apocConfig().setProperty(Geocode.PREFIX + ".provider", "anotherOne");
        apocConfig().setProperty(Geocode.PREFIX + ".opencage.key", "myOwnMockKey");
        final Map<String, Object> config = map("provider", "opencage",
                "url", URL, "reverseUrl", REVERSE_URL);
        geocodeOnceCommon(config);
    }

    @Test(expected = RuntimeException.class)
    public void testSimpleGeocodeWithWrongProvider() {
        try {
            // just to make sure that the spatial.json is well implemented
            // we pass a well-formatted url, reverse url and key but an incorrect provider
            final Map<String, Object> config = map("provider", "incorrect",
                    "url", URL, "reverseUrl", REVERSE_URL,
                    "key", "myOwnMockKey");
            geocodeOnceCommon(config);
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains(WRONG_PROVIDER_ERR));
            throw e;
        }
    }

    @Test
    public void testSimpleGeocodeOpencage() {
        final Map<String, Object> config = map("provider", "opencage",
                "url", URL, "reverseUrl", REVERSE_URL,
                "key", "myOwnMockKey");
        geocodeOnceCommon(config);
    }

    @Test
    public void testSimpleGeocodeGoogle() {
        final Map<String, Object> config = map("provider", "google");
        geocodeOnceCommon(config);
    }

    private void geocodeOnceCommon(Map<String, Object> config) {
        String query = "MATCH (a:Event) \n" +
                "WHERE exists(a.address) AND exists(a.name) \n" +
                "CALL apoc.spatial.geocodeOnce(a.address, $config) " +
                "YIELD latitude, longitude, description\n" +
                "WHERE latitude IS NOT NULL AND longitude IS NOT NULL AND description IS NOT NULL \n" +
                "RETURN *";
        testCallCount(db, query, map("config", config), eventNodes.size());
    }

    @Test
    public void testGeocodePointAndDistance() {
        String query = "WITH point({latitude: 48.8582532, longitude: 2.294287}) AS eiffel\n" +
                "MATCH (a:Event) \n" +
                "WHERE exists(a.address)\n" +
                "CALL apoc.spatial.geocodeOnce(a.address) YIELD location \n" +
                "WITH location, distance(point({latitude: location.latitude, longitude:location.longitude}), eiffel) AS distance\n" +
                "WHERE distance < 5000\n" +
                "RETURN location.description AS description, distance\n" +
                "ORDER BY distance\n" +
                "LIMIT 100\n";
        testCallCount(db, query, spaceNodes.size());
    }

    @Test
    public void testGraphRefactoring() {
        String refactorQuery = "MATCH (a:Event) \n" +
                "WHERE exists(a.address) AND NOT exists(a.latitude)\n" +
                "WITH a LIMIT 1000\n" +
                "CALL apoc.spatial.geocodeOnce(a.address) YIELD location \n" +
                "SET a.latitude = location.latitude\n" +
                "SET a.longitude = location.longitude";
        testCallEmpty(db, refactorQuery, emptyMap());
        String query = "WITH point({latitude: 48.8582532, longitude: 2.294287}) AS eiffel\n" +
                "MATCH (a:Event) \n" +
                "WHERE exists(a.latitude) AND exists(a.longitude)\n" +
                "WITH a, distance(point(a), eiffel) AS distance\n" +
                "WHERE distance < 5000\n" +
                "RETURN a.name AS event, distance\n" +
                "ORDER BY distance\n" +
                "LIMIT 100\n";
        testCallCount(db, query, spaceNodes.size());
    }

    @Test
    public void testAllTheThings() throws Exception {
        String query = "WITH apoc.date.parse('2016-06-01 00:00:00','h') as due_date,\n" +
                "     point({latitude: 48.8582532, longitude: 2.294287}) as eiffel\n" +
                "MATCH (e:Event)\n" +
                "WHERE exists(e.address) AND exists(e.datetime)\n" +
                "WITH apoc.date.parse(e.datetime,'h') as hours, e, due_date, eiffel\n" +
                "CALL apoc.spatial.geocodeOnce(e.address) YIELD location\n" +
                "WITH e, location,\n" +
                "     distance(point({longitude: location.longitude, latitude:location.latitude}), eiffel) as distance,\n" +
                "     (due_date - hours)/24.0 as days_before_due\n" +
                "WHERE distance < 5000 AND days_before_due < 14 AND hours < due_date\n" +
                "RETURN e.name as event, e.datetime as date,\n" +
                "       location.description as description, distance\n" +
                "ORDER BY distance\n" +
                "LIMIT 100\n";
        int expectedCount = spaceTimeNodes.size();
        testResult(db, query, res -> {
            int left = expectedCount;
            while (left > 0) {
                assertTrue("Expected " + expectedCount + " results, but got only " + (expectedCount - left),
                        res.hasNext());
                Map<String, Object> result = res.next();
                assertTrue("Event should have 'event' property", result.containsKey("event"));
                String name = (String) result.get("event");
                assertTrue("Event '" + name + "' was not expected", spaceTimeNodes.containsKey(name));
                left--;
            }
            assertFalse("Expected " + expectedCount + " results, but there are more ", res.hasNext());
        });
    }

    @Test
    public void testSimpleReverseGeocode() {
        final Map<String, Object> config = map();
        reverseGeocodeCommon(config);
    }

    @Test(expected = RuntimeException.class)
    public void testReverseGeocodeOpencageWrongUrlFormat() {
        // with provider different from osm/google we have to explicit an url correctly formatted (i.e. with 'PLACE' string)
        try {
            final Map<String, Object> conf = map("provider", "opencage",
                    "url", "wrongUrl",
                    "reverseUrl", REVERSE_URL);
            reverseGeocodeCommon(conf);
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains("Missing 'PLACE' in url template"));
            throw e;
        }
    }

    @Test(expected = RuntimeException.class)
    public void testReverseGeocodeOpencageMissingKey() {
        // with provider (via config map) different from osm/google we have to explicit the key
        try {
            final Map<String, Object> conf = map("provider", "opencage",
                    "url", URL, "reverseUrl", REVERSE_URL);
            reverseGeocodeCommon(conf);
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains("Missing 'key' for geocode provider"));
            throw e;
        }
    }

    @Test(expected = RuntimeException.class)
    public void testReverseGeocodeOpencageMissingKeyViaApocConfig() {
        // with provider(via apocConfig()) different from osm/google we have to explicit the key
        apocConfig().setProperty(Geocode.PREFIX + ".provider", "something");
        try {
            final Map<String, Object> conf = map("url", URL, "reverseUrl", REVERSE_URL);
            reverseGeocodeCommon(conf);
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains("Missing 'key' for geocode provider"));
            throw e;
        }
    }

    @Test
    public void testSimpleReverseGeocodeViaApocConfig() {
        // Missing key but doesn't fail because provider is google via ApocConfig, not opencage like testGeocodeOpencageMissingKey
        apocConfig().setProperty(Geocode.PREFIX + ".provider", "google");
        final Map<String, Object> config = map("url", "mockUrl", "reverseUrl", "mockReverseUrl");
        reverseGeocodeCommon(config);
    }

    @Test
    public void testSimpleReverseGeocodeOpencageOverwiteApocConfigs() {
        // the key is defined in apocConfig()
        // the url and provider are in both apocConfig() and config map, but the second ones win
        apocConfig().setProperty(Geocode.PREFIX + ".provider", "anotherOne");
        apocConfig().setProperty(Geocode.PREFIX + ".opencage.key", "myOwnMockKey");
        final Map<String, Object> config = map("provider", "opencage",
                "url", URL, "reverseUrl", REVERSE_URL);
        reverseGeocodeCommon(config);
    }

    @Test(expected = RuntimeException.class)
    public void testSimpleReverseGeocodeWithWrongProvider() {
        try {
            // just to make sure that the spatial.json is well implemented
            final Map<String, Object> config = map("provider", "incorrect",
                    "url", URL, "reverseUrl", REVERSE_URL,
                    "key", "myOwnMockKey");
            reverseGeocodeCommon(config);
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains(WRONG_PROVIDER_ERR));
            throw e;
        }
    }

    @Test
    public void testSimpleReverseGeocodeOpencage() {
        final Map<String, Object> config = map("provider", "opencage",
                "url", URL, "reverseUrl", REVERSE_URL,
                "key", "myOwnMockKey");
        reverseGeocodeCommon(config);
    }

    @Test
    public void testSimpleReverseGeocodeGoogle() {
        final Map<String, Object> config = map("provider", "google");
        reverseGeocodeCommon(config);
    }

    private void reverseGeocodeCommon(Map<String, Object> config) {
        String query = "MATCH (a:Event) \n" +
                "WHERE exists(a.lat) AND exists(a.lon) AND exists(a.name) \n" +
                "CALL apoc.spatial.reverseGeocode(a.lat, a.lon, false, $config) \n" +
                "YIELD latitude, longitude, description\n" +
                "WHERE latitude IS NOT NULL AND longitude IS NOT NULL AND description IS NOT NULL \n" +
                "RETURN *";
        testCallCount(db, query, map("config", config), eventNodes.size());
    }

    @Test
    public void testNullAddressErrorGeocodeOnce(){
        testCallEmpty(db, "CALL apoc.spatial.geocodeOnce(null)", emptyMap());
    }

    @Test
    public void testNullAddressErrorGeocodeShouldFail(){
        testCallEmpty(db, "CALL apoc.spatial.geocode(null,1)", emptyMap());
    }
}
