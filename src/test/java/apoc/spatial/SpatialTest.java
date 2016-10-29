package apoc.spatial;

import apoc.date.Date;
import apoc.util.JsonUtil;
import apoc.util.TestUtil;
import apoc.util.Util;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.URL;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.test.TestGraphDatabaseFactory;

import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCallCount;
import static apoc.util.TestUtil.testCallEmpty;
import static apoc.util.TestUtil.testResult;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SpatialTest {

    private GraphDatabaseService db;
    private Map<String, Map<String, Object>> eventNodes = new LinkedHashMap<>();
    private Map<String, Map<String, Object>> spaceNodes = new LinkedHashMap<>();
    private Map<String, Map<String, Object>> spaceTimeNodes = new LinkedHashMap<>();

    public static class MockGeocode {
        public static Map<String, Map> geocodeResults = null;

        public MockGeocode() {
        }

        @Procedure("apoc.spatial.geocodeOnce")
        public Stream<Geocode.GeoCodeResult> geocodeOnce(@Name("location") String address) {
            return geocode(address, 1);
        }

        @Procedure("apoc.spatial.geocode")
        public Stream<Geocode.GeoCodeResult> geocode(@Name("location") String address, @Name("maxResults") long maxResults) {
            if (geocodeResults != null && geocodeResults.containsKey(address)) {
                Map data = geocodeResults.get(address);
                return Stream.of(new Geocode.GeoCodeResult(Util.toDouble(data.get("lat")), Util.toDouble(data.get("lon")), String.valueOf(data.get("display_name")), data));
            } else {
                return Stream.empty();
            }
        }
    }

    @Before
    public void setUp() throws Exception {
        db = new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder().setConfig("apoc.import.file.enabled","true").newGraphDatabase();
        TestUtil.registerProcedure(db, Date.class);
        TestUtil.registerProcedure(db, MockGeocode.class);
        URL url = ClassLoader.getSystemResource("spatial.json");
        Map tests = (Map) JsonUtil.loadJson(url.toString());
        for (Object event : (List) tests.get("events")) {
            addEventData((Map) event);
        }
        MockGeocode.geocodeResults = (Map<String, Map>) tests.get("geocode");
    }

    private void addEventData(Map<String, Object> event) {
        Map<String, Object> params = map("params", event);
        Result result = db.execute("CREATE (e:Event {params})", params);
        int created = result.getQueryStatistics().getNodesCreated();
        assertTrue("Expected a node to be created", created == 1);
        String name = event.get("name").toString();
        if (!event.containsKey("toofar")) {
            spaceNodes.put(name, event);
            if (!event.containsKey("toosoon") && !event.containsKey("toolate")) {
                spaceTimeNodes.put(name, event);
            }
        }
        eventNodes.put(name, event);
    }

    @After
    public void tearDown() {
        db.shutdown();
    }

    @Test
    public void testSimpleGeocode() {
        String query = "MATCH (a:Event) \n" +
                "WHERE exists(a.address) AND exists(a.name) \n" +
                "CALL apoc.spatial.geocodeOnce(a.address) YIELD location\n" +
                "RETURN a.name, location.latitude AS latitude, \n" +
                "location.longitude AS longitude, location.description AS description";
        testCallCount(db, query, null, eventNodes.size());
    }

    @Test
    public void testGeocodePointAndDistance() {
        String query = "WITH point({latitude: 48.8582532, longitude: 2.294287}) AS eiffel\n" +
                "MATCH (a:Event) \n" +
                "WHERE exists(a.address)\n" +
                "CALL apoc.spatial.geocodeOnce(a.address) YIELD location\n" +
                "WITH location, distance(point(location), eiffel) AS distance\n" +
                "WHERE distance < 5000\n" +
                "RETURN location.description AS description, distance\n" +
                "ORDER BY distance\n" +
                "LIMIT 100\n";
        testCallCount(db, query, null, spaceNodes.size());
    }

    @Test
    public void testGraphRefactoring() {
        String refactorQuery = "MATCH (a:Event) \n" +
                "WHERE exists(a.address) AND NOT exists(a.latitude)\n" +
                "WITH a LIMIT 1000\n" +
                "CALL apoc.spatial.geocodeOnce(a.address) YIELD location \n" +
                "SET a.latitude = location.latitude\n" +
                "SET a.longitude = location.longitude";
        testCallEmpty(db, refactorQuery, null);
        String query = "WITH point({latitude: 48.8582532, longitude: 2.294287}) AS eiffel\n" +
                "MATCH (a:Event) \n" +
                "WHERE exists(a.latitude) AND exists(a.longitude)\n" +
                "WITH a, distance(point(a), eiffel) AS distance\n" +
                "WHERE distance < 5000\n" +
                "RETURN a.name AS event, distance\n" +
                "ORDER BY distance\n" +
                "LIMIT 100\n";
        testCallCount(db, query, null, spaceNodes.size());
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
                "     distance(point(location), eiffel) as distance,\n" +
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
}
