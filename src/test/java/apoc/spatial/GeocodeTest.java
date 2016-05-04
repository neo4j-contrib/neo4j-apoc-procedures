package apoc.spatial;

import apoc.util.JsonUtil;
import apoc.util.TestUtil;
import org.junit.After;
import org.junit.Assert;
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

    @Test
    public void testGeocode() throws Exception {
        URL url = ClassLoader.getSystemResource("spatial.json");
        Map tests = (Map) JsonUtil.OBJECT_MAPPER.readValue(url.openConnection().getInputStream(), Object.class);
        for (Object address : (List) tests.get("addresses")) {
            testGeocodeAddress((Map) address);
        }
    }

    private void testGeocodeAddress(Map map) {
        if (map.containsKey("noresults")) {
            for (String field : new String[]{"address", "noresults"}) {
                Assert.assertTrue("Expected " + field + " field", map.containsKey(field));
            }
            testCallEmpty(db, "CALL apoc.spatial.geocode({url})", map("url", map.get("address").toString()));
        } else if (map.containsKey("count")) {
            for (String field : new String[]{"address", "count"}) {
                Assert.assertTrue("Expected " + field + " field", map.containsKey(field));
            }
            testCallCount(db, "CALL apoc.spatial.geocode({url})",
                    map("url", map.get("address").toString()),
                    (int) map.get("count"));
        } else {
            for (String field : new String[]{"address", "latitude", "longitude"}) {
                Assert.assertTrue("Expected " + field + " field", map.containsKey(field));
            }
            testGeocodeAddress(map.get("address").toString(),
                    (double) map.get("latitude"),
                    (double) map.get("longitude"));
        }
    }

    private void testGeocodeAddress(String address, double lat, double lon) {
        testCall(db, "CALL apoc.spatial.geocode({url})", map("url", address),
                (row) -> {
                    Map value = (Map) row.get("location");
                    assertEquals("Incorrect latitude found", lat, Double.parseDouble(value.get("latitude").toString()),
                            0.0001);
                    assertEquals("Incorrect longitude found", lon, Double.parseDouble(value.get("longitude").toString()),
                            0.0001);
                });
    }
}
