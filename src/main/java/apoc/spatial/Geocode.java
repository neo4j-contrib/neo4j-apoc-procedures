package apoc.spatial;

import apoc.Description;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import apoc.util.JsonUtil;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import static apoc.util.MapUtil.map;

public class Geocode {
    @Context
    public GraphDatabaseService db;

    @Procedure
    @Description("apoc.spatial.geocode('address') YIELD location, latitude, longitude, description, osmData - look up geographic location of address from openstreetmap geocoding service")
    public Stream<GeoCodeResult> geocode(@Name("location") String address) {
        String url = "http://nominatim.openstreetmap.org/search.php?q=" + address.replace(" ", "+") + "&format=json";
        Object value = JsonUtil.loadJson(url);
        if (value instanceof List) {
            return ((List<Map<String, Object>>) value).stream().map(data ->
                    new GeoCodeResult(toDouble(data.get("lat")), toDouble(data.get("lon")), String.valueOf(data.get("display_name")), data));
        }
        throw new RuntimeException("Can't parse geocoding results " + value);
    }

    public static Double toDouble(Object value) {
        if (value == null) return null;
        if (value instanceof Number) return ((Number) value).doubleValue();
        return Double.parseDouble(value.toString());
    }

    public static class GeoCodeResult {
        public final Map<String, Object> location;
        public final Map<String, Object> osmData;
        public final Double latitude;
        public final Double longitude;
        public final String description;

        public GeoCodeResult(Double latitude, Double longitude, String description, Map<String, Object> osmData) {
            this.osmData = osmData;
            this.latitude = latitude;
            this.longitude = longitude;
            this.description = description;
            this.location = map("latitude", latitude, "longitude", longitude, "description", description);
        }
    }
}
