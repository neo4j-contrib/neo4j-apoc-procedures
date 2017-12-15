package apoc.spatial;

import apoc.ApocConfiguration;
import org.neo4j.procedure.*;
import apoc.util.JsonUtil;
import apoc.util.Util;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.logging.Log;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static apoc.util.MapUtil.map;
import static apoc.util.Util.toDouble;
import static apoc.util.Util.toLong;
import static java.lang.String.valueOf;
import static java.lang.System.currentTimeMillis;

public class Geocode {
    public static final int MAX_RESULTS = 100;
    public static final String PREFIX = "spatial.geocode";
    public static final String GEOCODE_PROVIDER_KEY = "provider";

    @Context
    public GraphDatabaseService db;

    @Context
    public TerminationGuard terminationGuard;

    @Context
    public Log log;

    interface GeocodeSupplier {
        Stream<GeoCodeResult> geocode(String encodedAddress, long maxResults);
    }

    private static class Throttler {
        private final TerminationGuard terminationGuard;
        private long throttleInMs;
        private static long lastCallTime = 0L;
        private static long DEFAULT_THROTTLE = 5*1000;  // 5 seconds
        private static long MAX_THROTTLE = 60 * 60 * 1000;  // 1 hour

        public Throttler(TerminationGuard terminationGuard, long throttle) {
            this.terminationGuard = terminationGuard;

            throttle = Math.min(throttle, MAX_THROTTLE);
            if (throttle < 0) throttle = DEFAULT_THROTTLE;

            this.throttleInMs = throttle;
        }

        private void waitForThrottle() {
            long msSinceLastCall = currentTimeMillis() - lastCallTime;
            while (msSinceLastCall < throttleInMs) {
                try {
                    terminationGuard.check();
                    long msToWait = throttleInMs - msSinceLastCall;
                    Thread.sleep(Math.min(msToWait, 1000));
                } catch (InterruptedException e) {
                    // ignore
                }
                msSinceLastCall = currentTimeMillis() - lastCallTime;
            }
            lastCallTime = currentTimeMillis();
        }
    }

    private static class SupplierWithKey implements GeocodeSupplier {
        private static final String[] FORMATTED_KEYS = new String[]{"formatted", "formatted_address", "address", "description", "display_name"};
        private static final String[] LAT_KEYS = new String[]{"lat", "latitude"};
        private static final String[] LNG_KEYS = new String[]{"lng", "longitude", "lon"};
        private Throttler throttler;
        private String configBase;
        private String urlTemplate;

        public SupplierWithKey(Map<String, Object> config, TerminationGuard terminationGuard, String provider) {
            this.configBase = provider;

            if (!config.containsKey(configKey("url"))) {
                throw new IllegalArgumentException("Missing 'url' for geocode provider: " + provider);
            }
            urlTemplate = config.get(configKey("url")).toString();
            if (!urlTemplate.contains("PLACE")) throw new IllegalArgumentException("Missing 'PLACE' in url template: " + urlTemplate);

            if (urlTemplate.contains("KEY") && !config.containsKey(configKey("key"))) {
                throw new IllegalArgumentException("Missing 'key' for geocode provider: " + provider);
            }
            String key = config.get(configKey("key")).toString();
            urlTemplate = urlTemplate.replace("KEY", key);

            this.throttler = new Throttler(terminationGuard, toLong(ApocConfiguration.get(configKey("throttle"), Throttler.DEFAULT_THROTTLE)));
        }

        @SuppressWarnings("unchecked")
        public Stream<GeoCodeResult> geocode(String address, long maxResults) {
            throttler.waitForThrottle();
            String url = urlTemplate.replace("PLACE", Util.encodeUrlComponent(address));
            Object value = JsonUtil.loadJson(url).findFirst().orElse(null);
            if (value instanceof List) {
                return findResults((List<Map<String, Object>>) value, maxResults);
            } else if (value instanceof Map) {
                Object results = ((Map) value).get("results");
                if (results instanceof List) {
                    return findResults((List<Map<String, Object>>) results, maxResults);
                }
            }
            throw new RuntimeException("Can't parse geocoding results " + value);
        }

        @SuppressWarnings("unchecked")
        private Stream<GeoCodeResult> findResults(List<Map<String, Object>> results, long maxResults) {
            return results.stream().limit(maxResults).map(data -> {
                String description = findFirstEntry(data, FORMATTED_KEYS);
                Map<String,Object> location = (Map<String,Object>) data.get("geometry");
                if (location.containsKey("location")) {
                    location = (Map<String,Object>) location.get("location");
                }
                String lat = findFirstEntry(location, LAT_KEYS);
                String lng = findFirstEntry(location, LNG_KEYS);
                return new GeoCodeResult(toDouble(lat), toDouble(lng), description, data);
            });
        }

        private String findFirstEntry(Map<String, Object> map, String[] keys) {
            for (String key : keys) {
                if (map.containsKey(key)) {
                    return valueOf(map.get(key));
                }
            }
            return "";
        }

        private String configKey(String name) {
            return configBase + "." + name;
        }

    }

    private static class OSMSupplier implements GeocodeSupplier {
        public static final String OSM_URL = "http://nominatim.openstreetmap.org/search.php?format=json&q=";
        private Throttler throttler;

        public OSMSupplier(Map<String, Object> config, TerminationGuard terminationGuard) {
            this.throttler = new Throttler(terminationGuard, toLong(config.getOrDefault("osm.throttle", Throttler.DEFAULT_THROTTLE)));
        }

        @SuppressWarnings("unchecked")
        public Stream<GeoCodeResult> geocode(String address, long maxResults) {
            throttler.waitForThrottle();
            Object value = JsonUtil.loadJson(OSM_URL + Util.encodeUrlComponent(address)).findFirst().orElse(null);
            if (value instanceof List) {
                return ((List<Map<String, Object>>) value).stream().limit(maxResults).map(data ->
                        new GeoCodeResult(toDouble(data.get("lat")), toDouble(data.get("lon")), valueOf(data.get("display_name")), data));
            }
            throw new RuntimeException("Can't parse geocoding results " + value);
        }
    }

    class GoogleSupplier implements GeocodeSupplier {
        private final Throttler throttler;
        private String baseUrl;

        public GoogleSupplier(Map<String, Object> config, TerminationGuard terminationGuard) {
            this.throttler = new Throttler(terminationGuard, toLong(config.getOrDefault("google.throttle", Throttler.DEFAULT_THROTTLE)));
            this.baseUrl = String.format("https://maps.googleapis.com/maps/api/geocode/json?%s&address=", credentials(config));
        }

        private String credentials(Map<String, Object> config) {
            if (config.containsKey("google.client") && config.containsKey("google.signature")) {
                return "client=" + config.get("google.client") + "&signature=" + config.get("google.signature");
            } else if (config.containsKey("google.key")) {
                return "key=" + config.get("google.key");
            } else {
                return "auth=free"; // throw new RuntimeException("apoc.spatial.geocode: No google client or key specified in neo4j.conf config file");
            }
        }

        @SuppressWarnings("unchecked")
        public Stream<GeoCodeResult> geocode(String address, long maxResults) {
            if (address.length() < 1) {
                return Stream.empty();
            }
            throttler.waitForThrottle();
            Object value = JsonUtil.loadJson(baseUrl + Util.encodeUrlComponent(address)).findFirst().orElse(null);
            if (value instanceof Map) {
                Map map = (Map) value;
                if (map.get("status").equals("OVER_QUERY_LIMIT")) throw new IllegalStateException("QUOTA_EXCEEDED from geocode API: "+map.get("status")+" message: "+map.get("error_message"));
                Object results = map.get("results");
                if (results instanceof List) {
                    return ((List<Map<String, Object>>) results).stream().limit(maxResults).map(data -> {
                        Map location = (Map) ((Map) data.get("geometry")).get("location");
                        return new GeoCodeResult(toDouble(location.get("lat")), toDouble(location.get("lng")), valueOf(data.get("formatted_address")), data);
                    });
                }
            }
            throw new RuntimeException("Can't parse geocoding results " + value);
        }
    }

    private GeocodeSupplier getSupplier() {
        Map<String, Object> activeConfig = ApocConfiguration.get(PREFIX);
        if (activeConfig.containsKey(GEOCODE_PROVIDER_KEY)) {
            String supplier = activeConfig.get(GEOCODE_PROVIDER_KEY).toString().toLowerCase();
            switch (supplier) {
                case "google" : return new GoogleSupplier(activeConfig, terminationGuard);
                case "osm" : return new OSMSupplier(activeConfig,terminationGuard);
                default: return new SupplierWithKey(activeConfig, terminationGuard, supplier);
            }
        }
        return new OSMSupplier(activeConfig, terminationGuard);
    }

    @Procedure
    @Description("apoc.spatial.geocodeOnce('address') YIELD location, latitude, longitude, description, osmData - look up geographic location of address from openstreetmap geocoding service")
    public Stream<GeoCodeResult> geocodeOnce(@Name("location") String address) throws UnsupportedEncodingException {
        return geocode(address, 1L,false);
    }

    @Procedure
    @Description("apoc.spatial.geocode('address') YIELD location, latitude, longitude, description, osmData - look up geographic location of address from openstreetmap geocoding service")
    public Stream<GeoCodeResult> geocode(@Name("location") String address, @Name(value = "maxResults",defaultValue = "100") long maxResults, @Name(value = "quotaException",defaultValue = "false") boolean quotaException) {
        try {
            return getSupplier().geocode(address, maxResults == 0 ? MAX_RESULTS : Math.min(Math.max(maxResults, 1), MAX_RESULTS));
        } catch(IllegalStateException re) {
            if (!quotaException && re.getMessage().startsWith("QUOTA_EXCEEDED")) return Stream.empty();
            throw re;
        }
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
