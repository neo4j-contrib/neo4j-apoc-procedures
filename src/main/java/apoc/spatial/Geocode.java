package apoc.spatial;

import apoc.Description;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import apoc.util.JsonUtil;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import static apoc.util.MapUtil.map;

public class Geocode {
    @Context
    public GraphDatabaseService db;

    @Context
    public GraphDatabaseAPI graph;

    @Context
    public KernelTransaction kernelTransaction;

    interface GeocodeSupplier {
        public Stream<GeoCodeResult> geocode(String address, long maxResults);
    }

    private static class OSMSupplier implements GeocodeSupplier {
        private final KernelTransaction kernelTransaction;
        private static long DEFAULT_THROTTLE = 5000;  // 5 seconds
        private static long MAX_THROTTLE = 1000 * 60 * 60;  // 1 hour
        private long throttle = DEFAULT_THROTTLE;
        private static long lastCallTime = 0L;

        public OSMSupplier(Map<String, String> config, KernelTransaction kernelTransaction) {
            this.kernelTransaction = kernelTransaction;
            if (config.containsKey("apoc.spatial.geocode.osm.throttle")) {
                throttle = Long.parseLong(config.get("apoc.spatial.geocode.osm.throttle"));
                if (throttle < 0) {
                    throttle = DEFAULT_THROTTLE;
                }
                if (throttle > MAX_THROTTLE) {
                    throttle = DEFAULT_THROTTLE;
                }
            }
        }

        private void waitForThrottle() {
            long msSinceLastCall = System.currentTimeMillis() - lastCallTime;
            while (msSinceLastCall < throttle) {
                if (kernelTransaction.shouldBeTerminated()) {
                    throw new RuntimeException("geocode called in terminated transaction");
                }
                try {
                    System.out.println("apoc.spatial.geocode: throttling calls to OSM service");
                    Thread.sleep(Math.min(throttle - msSinceLastCall, 1000));
                } catch (InterruptedException e) {
                }
                msSinceLastCall = System.currentTimeMillis() - lastCallTime;
            }
            lastCallTime = System.currentTimeMillis();
        }

        public Stream<GeoCodeResult> geocode(String address, long maxResults) {
            waitForThrottle();
            String url = "http://nominatim.openstreetmap.org/search.php?q=" + address.replace(" ", "+") + "&format=json";
            System.out.println("apoc.spatial.geocode: " + url);
            Object value = JsonUtil.loadJson(url);
            if (value instanceof List) {
                return ((List<Map<String, Object>>) value).stream().limit(maxResults).map(data ->
                        new GeoCodeResult(toDouble(data.get("lat")), toDouble(data.get("lon")), String.valueOf(data.get("display_name")), data));
            }
            throw new RuntimeException("Can't parse geocoding results " + value);
        }
    }

    class GoogleSupplier implements GeocodeSupplier {
        private final KernelTransaction kernelTransaction;
        private String credentials = "";

        public GoogleSupplier(Map<String, String> config, KernelTransaction kernelTransaction) {
            this.kernelTransaction = kernelTransaction;
            if (config.containsKey("apoc.spatial.geocode.google.client") && config.containsKey("apoc.spatial.geocode.google.signature")) {
                credentials = "&client=" + config.containsKey("apoc.spatial.geocode.google.client") + "&signature=" + config.containsKey("apoc.spatial.geocode.google.signature");
            } else if (config.containsKey("apoc.spatial.geocode.google.key")) {
                credentials = "&key=" + config.containsKey("apoc.spatial.geocode.google.key");
            } else {
                System.err.println("apoc.spatial.geocode: No google client or key specified in neo4j config file");
            }
        }

        public Stream<GeoCodeResult> geocode(String address, long maxResults) {
            if (address.length() < 1) {
                return Stream.empty();
            }
            String url = "https://maps.googleapis.com/maps/api/geocode/json?address=" + address.replace(" ", "+") + credentials;
            System.out.println("apoc.spatial.geocode: " + url);
            Object value = JsonUtil.loadJson(url);
            if (value instanceof Map) {
                Object results = ((Map) value).get("results");
                if (results instanceof List) {
                    return ((List<Map<String, Object>>) results).stream().limit(maxResults).map(data -> {
                        Map location = (Map) ((Map) data.get("geometry")).get("location");
                        return new GeoCodeResult(toDouble(location.get("lat")), toDouble(location.get("lng")), String.valueOf(data.get("formatted_address")), data);
                    });
                }
            }
            throw new RuntimeException("Can't parse geocoding results " + value);
        }
    }

    private Map<String, String> getConfig() {
        return graph.getDependencyResolver().resolveDependency(Config.class).getParams();
    }

    private GeocodeSupplier getSupplier() {
        Map<String, String> config = getConfig();
        if (config.containsKey("apoc.spatial.geocode.supplier")) {
            if (config.get("apoc.spatial.geocode.supplier").toLowerCase().startsWith("google")) {
                return new GoogleSupplier(config, kernelTransaction);
            }
        }
        return new OSMSupplier(config, kernelTransaction);
    }

    @Procedure
    @Description("apoc.spatial.geocodeOnce('address') YIELD location, latitude, longitude, description, osmData - look up geographic location of address from openstreetmap geocoding service")
    public Stream<GeoCodeResult> geocodeOnce(@Name("location") String address) {
        return geocode(address, 1L);
    }

    @Procedure
    @Description("apoc.spatial.geocode('address') YIELD location, latitude, longitude, description, osmData - look up geographic location of address from openstreetmap geocoding service")
    public Stream<GeoCodeResult> geocode(@Name("location") String address, @Name("maxResults") Long maxResults) {
        if (maxResults < 0) {
            throw new RuntimeException("Invalid maximum number of results requested: " + maxResults);
        }
        if (maxResults == 0) {
            maxResults = Long.MAX_VALUE;
        }
        return getSupplier().geocode(address, maxResults);
    }

    public static Double toDouble(Object value) {
        if (value == null) return null;
        if (value instanceof Number) return ((Number) value).doubleValue();
        return Double.parseDouble(value.toString());
    }

    public static class GeoCodeResult {
        public final Map<String, Object> location;
        public final Map<String, Object> data;
        public final Double latitude;
        public final Double longitude;
        public final String description;

        public GeoCodeResult(Double latitude, Double longitude, String description, Map<String, Object> data) {
            this.data = data;
            this.latitude = latitude;
            this.longitude = longitude;
            this.description = description;
            this.location = map("latitude", latitude, "longitude", longitude, "description", description);
        }
    }
}
