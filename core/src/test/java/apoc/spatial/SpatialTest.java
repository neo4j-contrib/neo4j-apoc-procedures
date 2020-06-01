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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.*;
import static java.util.Collections.emptyMap;
import static org.junit.Assert.*;

public class SpatialTest {

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
        public Stream<Geocode.GeoCodeResult> geocodeOnce(@Name("location") String address) {
            return geocode(address, 1);
        }

        @Procedure("apoc.spatial.geocode")
        public Stream<Geocode.GeoCodeResult> geocode(@Name("location") String address, @Name("maxResults") long maxResults) {
            if (address == null || address.isEmpty())
                return Stream.empty();
            else {
                if (geocodeResults != null && geocodeResults.containsKey(address)) {
                    Map data = geocodeResults.get(address);
                    return Stream.of(new Geocode.GeoCodeResult(Util.toDouble(data.get("lat")), Util.toDouble(data.get("lon")), String.valueOf(data.get("display_name")), data));
                } else {
                    return Stream.empty();
                }
            }
        }

        @Procedure("apoc.spatial.reverseGeocode")
        public Stream<Geocode.GeoCodeResult> reverseGeocode(@Name("latitude") double latitude, @Name("longitude") double longitude, @Name(value = "maxResults", defaultValue = "100") long maxResults) {
            String key = latitude + "," + longitude;
            if (reverseGeocodeResults != null && reverseGeocodeResults.containsKey(key)) {
                Map data = reverseGeocodeResults.get(key);
                return Stream.of(new Geocode.GeoCodeResult(latitude, longitude, String.valueOf(data.get("display_name")), data));
            } else {
                return Stream.empty();
            }
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
        String query = "MATCH (a:Event) \n" +
                "WHERE exists(a.address) AND exists(a.name) \n" +
                "CALL apoc.spatial.geocodeOnce(a.address) YIELD location\n" +
                "RETURN a.name, location.latitude AS latitude, \n" +
                "location.longitude AS longitude, location.description AS description";
        testCallCount(db, query, eventNodes.size());
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
        String query = "MATCH (a:Event) \n" +
                "WHERE exists(a.lat) AND exists(a.lon) AND exists(a.name) \n" +
                "CALL apoc.spatial.reverseGeocode(a.lat, a.lon) YIELD latitude, longitude\n" +
                "RETURN a.name, latitude, \n" +
                "longitude, a.description";
        testCallCount(db, query, eventNodes.size());
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
