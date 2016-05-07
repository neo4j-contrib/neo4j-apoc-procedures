package apoc.spatial;

import apoc.Description;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import apoc.result.MapResult;
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

    private static final Map<String, String> config = new LinkedHashMap<>();

    interface GeocodeSupplier {
        public Stream<GeoCodeResult> geocode(String address, long maxResults);
    }

    private static class Throttler {
        private final KernelTransaction kernelTransaction;
        private static long DEFAULT_THROTTLE = 5000;  // 5 seconds
        private static long MAX_THROTTLE = 1000 * 60 * 60;  // 1 hour
        private long throttle;
        private static long lastCallTime = 0L;

        public Throttler(Map<String, String> config, KernelTransaction kernelTransaction, String throttleKey, long defaultThrottle) {
            this.throttle = defaultThrottle;
            this.kernelTransaction = kernelTransaction;
            if (config.containsKey(throttleKey)) {
                throttle = Long.parseLong(config.get(throttleKey));
                if (throttle < 0) {
                    throttle = defaultThrottle;
                }
                if (throttle > MAX_THROTTLE) {
                    throttle = defaultThrottle;
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
                    long msToWait = throttle - msSinceLastCall;
                    System.out.println("apoc.spatial.geocode: throttling calls to geocode service for " + msToWait + "ms");
                    Thread.sleep(Math.min(msToWait, 1000));
                } catch (InterruptedException e) {
                }
                msSinceLastCall = System.currentTimeMillis() - lastCallTime;
            }
            lastCallTime = System.currentTimeMillis();
        }
    }

    private static class OSMSupplier implements GeocodeSupplier {
        private Throttler throttler;

        public OSMSupplier(Map<String, String> config, KernelTransaction kernelTransaction) {
            this.throttler = new Throttler(config, kernelTransaction, "apoc.spatial.geocode.osm.throttle", Throttler.DEFAULT_THROTTLE);
        }

        public Stream<GeoCodeResult> geocode(String address, long maxResults) {
            throttler.waitForThrottle();
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
        private final Throttler throttler;
        private String credentials = "";
        private static final String GOOGLE_KEY = "apoc.spatial.geocode.google.key";
        private static final String GOOGLE_CLIENT = "apoc.spatial.geocode.google.client";
        private static final String GOOGLE_SIGNATURE = "apoc.spatial.geocode.google.signature";

        public GoogleSupplier(Map<String, String> config, KernelTransaction kernelTransaction) {
            this.throttler = new Throttler(config, kernelTransaction, "apoc.spatial.geocode.google.throttle", 1);
            if (config.containsKey(GOOGLE_CLIENT) && config.containsKey(GOOGLE_SIGNATURE)) {
                credentials = "&client=" + config.containsKey(GOOGLE_CLIENT) + "&signature=" + config.containsKey(GOOGLE_SIGNATURE);
            } else if (config.containsKey(GOOGLE_KEY)) {
                credentials = "&key=" + config.containsKey(GOOGLE_KEY);
            } else {
                System.err.println("apoc.spatial.geocode: No google client or key specified in neo4j config file");
            }
        }

        public Stream<GeoCodeResult> geocode(String address, long maxResults) {
            if (address.length() < 1) {
                return Stream.empty();
            }
            throttler.waitForThrottle();
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

    private void addSpatialConfigs(Map<String, String> map, Map<String, String> other) {
        for (String key : other.keySet()) {
            if (key.startsWith("apoc.spatial")) {
                map.put(key, other.get(key));
            }
        }
    }

    private Map<String, String> getConfig() {
        HashMap<String, String> activeConfig = new LinkedHashMap<>();
        addSpatialConfigs(activeConfig, graph.getDependencyResolver().resolveDependency(Config.class).getParams());
        addSpatialConfigs(activeConfig, Geocode.config);
        return activeConfig;
    }

    public static final String GEOCODE_SUPPLIER_KEY = "apoc.spatial.geocode.provider";

    private GeocodeSupplier getSupplier() {
        Map<String, String> activeConfig = getConfig();
        if (activeConfig.containsKey(GEOCODE_SUPPLIER_KEY)) {
            if (activeConfig.get(GEOCODE_SUPPLIER_KEY).toLowerCase().startsWith("google")) {
                return new GoogleSupplier(activeConfig, kernelTransaction);
            }
        }
        return new OSMSupplier(activeConfig, kernelTransaction);
    }

    @Procedure
    @Description("apoc.spatial.config(map) - configure spatial procedures")
    public Stream<MapResult> config(@Name("config") Map<String, String> config) {
        Geocode.config.clear();
        addSpatialConfigs(Geocode.config, config);
        return Stream.of(new MapResult(getConfig()));
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
