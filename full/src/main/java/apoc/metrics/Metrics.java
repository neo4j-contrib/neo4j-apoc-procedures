package apoc.metrics;

import apoc.Extended;
import apoc.export.util.CountingReader;
import apoc.load.CSVResult;
import apoc.load.LoadCsv;
import apoc.load.util.LoadCsvConfig;
import apoc.util.FileUtils;
import apoc.util.Util;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.io.File;
import java.io.FilenameFilter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static apoc.ApocConfig.apocConfig;
import static apoc.util.FileUtils.closeReaderSafely;

/**
 * @author moxious
 * @since 27.02.19
 */
@Extended
public class Metrics {
    @Context
    public Log log;

    /** Simple DAO that pairs a config setting name with a File path that it refers to */
    public static class StoragePair {
        public final String setting;
        public final File dir;
        public StoragePair(String setting, File dir) {
            this.setting = setting;
            this.dir = dir;
        }

        /** Produce a StoragePair from a directory setting name, such as "dbms.directories.logs" */
        public static StoragePair fromDirectorySetting(String dir) {
            if (dir == null) return null;

            String configLocation = apocConfig().getString(dir, null);
            if (configLocation == null) return null;

            File f = new File(configLocation);
            return new StoragePair(dir, f);
        }
    }

    /**
     * DAO that gets streamed to the user with apoc.metric.storage()
     * Note that we divulge the storage directory setting, not the internal path.
     * It isn't a secret that neo4j has a dbms.directories.logs, but user doesn't need
     * to know where that is on a physical hard disk.
     */
    public static class StorageMetric {
        public final String setting;
        public final long freeSpaceBytes;
        public final long totalSpaceBytes;
        public final long usableSpaceBytes;
        public final double percentFree;

        public StorageMetric(String setting, long freeSpaceBytes, long totalSpaceBytes, long usableSpaceBytes) {
            this.setting = setting;
            this.freeSpaceBytes = freeSpaceBytes;
            this.totalSpaceBytes = totalSpaceBytes;
            this.usableSpaceBytes = usableSpaceBytes;
            this.percentFree = (totalSpaceBytes <= 0) ? 0.0 : ((double)freeSpaceBytes / (double)totalSpaceBytes);
        }

        /** Produce a StorageMetric object from a pair */
        public static StorageMetric fromStoragePair(StoragePair storagePair) {
            long freeSpace = storagePair.dir.getFreeSpace();
            long usableSpace = storagePair.dir.getUsableSpace();
            long totalSpace = storagePair.dir.getTotalSpace();
            return new StorageMetric(storagePair.setting, freeSpace, totalSpace, usableSpace);
        }
    }

    /**
     * DAO for a single line in a metrics/*.csv file.
     * Value is abstracted to double; in reality some are int, some are float, some are double.
     */
    public static class GenericMetric {
        public final long timestamp;
        public final String metric;
        public final Map<String,Object> map;

        public GenericMetric(String metric, long t, Map<String,Object> map) {
            this.timestamp = t;
            this.metric = metric;
            this.map = map;
        }
    }

    public static class Neo4jMeasuredMetric {
        public final String name;
        public final long lastUpdated;

        public Neo4jMeasuredMetric(String name, long lastUpdated) {
            this.name = name;
            this.lastUpdated = lastUpdated;
        }
    }

    @Procedure(mode=Mode.DBMS)
    @Description("apoc.metrics.list() - get a list of available metrics")
    public Stream<Neo4jMeasuredMetric> list() {
        File metricsDir = FileUtils.getMetricsDirectory();

        final FilenameFilter filter = (dir, name) -> name.toLowerCase().endsWith(".csv");
        return Arrays.asList(metricsDir.listFiles(filter))
                .stream()
                .map(metricFile -> {
                    String name = metricFile.getName();
                    String metricName = name.substring(0, name.length() - 4);
                    File f = new File(metricsDir, name);
                    return new Neo4jMeasuredMetric(metricName, f.lastModified());
                });
    }

    /**
     * Neo4j CSV metrics have an issue where sometimes in the middle of the CSV file you'll find an extra
     * header row.  We want to discard those.
     */
    private static final Predicate<CSVResult> duplicatedHeaderRows = new Predicate <CSVResult> () {
        public boolean test(CSVResult o) {
            if (o == null) return false;

            Map<String,Object> map = o.map;

            // Most commonly CSV has a timestamp "t" field, if its value = "t"
            // Then it's a repeated header.  This is just a shortcut for the most common
            // case to avoid checking entire map every time.  If the value of the "t" field
            // is null, it's also a repeated header or bad row.  We're specifying type mappings
            // from the CSV string t -> long.  So if the actual value is "t" this will turn into
            // a null long.
            if ("t".equals(map.get("t"))) {
                return false;
            } else {
                for(Object value : map.values()) {
                    if (value instanceof Number) {
                        // Any value which is a number got type converted, and is not a header.
                        return true;
                    }
                }
            }

            // Final case: no number data.  Likely all null values, as headers failed type mapping to
            // numbers.
            return false;
        }
    };

    public Stream<GenericMetric> loadCsvForMetric(String metricName, Map<String,Object> config) {
        // These config parameters are generally true of Neo4j metrics.
        config.put("sep", ",");
        config.put("header", true);

        File metricsDir = FileUtils.getMetricsDirectory();

        if (metricsDir == null) {
            throw new RuntimeException("Metrics directory either does not exist or is not readable.  " +
                    "To use this procedure please ensure CSV metrics are configured " +
                    "https://neo4j.com/docs/operations-manual/current/monitoring/metrics/expose/#metrics-csv");
        }

        String url = new File(metricsDir, metricName + ".csv").getAbsolutePath();
        CountingReader reader = null;
        try {
            reader = FileUtils.SupportedProtocols.file
                    .getStreamConnection(url, null, null)
                    .toCountingInputStream()
                    .asReader();
            return new LoadCsv()
                    .streamCsv(url, new LoadCsvConfig(config), reader)
                    .filter(Metrics.duplicatedHeaderRows)
                    .map(csvResult -> new GenericMetric(metricName, Util.toLong(csvResult.map.get("t")), csvResult.map));
        } catch (Exception e) {
            closeReaderSafely(reader);
            throw new RuntimeException(e);
        }
    }

    @Procedure(mode=Mode.DBMS)
    @Description("apoc.metrics.storage(directorySetting) - retrieve storage metrics about the devices Neo4j uses for data storage. " +
            "directorySetting may be any valid neo4j directory setting name, such as 'dbms.directories.data'.  If null is provided " +
            "as a directorySetting, you will get back all available directory settings.  For a list of available directory settings, " +
            "see the Neo4j operations manual reference on configuration settings.   Directory settings are **not** paths, they are " +
            "a neo4j.conf setting key name")
    public Stream<StorageMetric> storage(@Name("directorySetting") String directorySetting) {
        // Permit case-insensitive checks.
        String input = directorySetting == null ? null : directorySetting.toLowerCase();

        boolean validSetting = input == null || FileUtils.NEO4J_DIRECTORY_CONFIGURATION_SETTING_NAMES.contains(input);

        if (!validSetting) {
            String validOptions = String.join(", ", FileUtils.NEO4J_DIRECTORY_CONFIGURATION_SETTING_NAMES);
            throw new RuntimeException("Invalid directory setting specified.  Valid options are one of: " +
                    validOptions);
        }

        return FileUtils.NEO4J_DIRECTORY_CONFIGURATION_SETTING_NAMES.stream()
                // If user specified a particular one, immediately cut list to just that one.
                .filter(dirSetting -> (input == null || input.equals(dirSetting)))
                .map(StoragePair::fromDirectorySetting)
                .filter(sp -> {
                    if (sp == null) { return false; }

                    if (sp.dir.exists() && sp.dir.isDirectory() && sp.dir.canRead()) {
                        return true;
                    }

                    log.warn("System directory " + sp.setting + " => " + sp.dir + " does not exist or is not readable.");
                    return false;
                })
                .map(StorageMetric::fromStoragePair);
    }

    @Procedure(mode=Mode.DBMS)
    @Description("apoc.metrics.get(metricName, {}) - retrieve a system metric by its metric name. Additional configuration options may be passed matching the options available for apoc.load.csv.")
    /**
     * This method is a specialization of apoc.load.csv, it just happens that the directory and file path
     * are local.  For Neo4j metrics, see: https://neo4j.com/docs/operations-manual/current/monitoring/metrics/reference/
     *
     * The reason it's not suitable for users to use regular apoc.load.csv to get at the metrics file is that from the
     * outside they have no way of knowing the home path where they're located, and apoc.load.csv doesn't allow
     * relative paths.
     */
    public Stream<GenericMetric> get(
            @Name("metricName") String metricName,
            @Name(value = "config",defaultValue = "{}") Map<String, Object> config) {

        Map<String,Object> csvConfig = config;

        if(csvConfig == null) {
            csvConfig = new HashMap<String,Object>();
        }

        // Add default mappings for metrics only if user hasn't overridden them.
        if (!csvConfig.containsKey("mapping")) {
            csvConfig.put("mapping", METRIC_TYPE_MAPPINGS);
        }

        return loadCsvForMetric(metricName, csvConfig);
    }

    private static final Map<String,Object> METRIC_TYPE_MAPPINGS = new HashMap<String,Object>();
    static {
        final Map<String,String> typeFloat = new HashMap<String,String>();
        typeFloat.put("type", "float");  // "float" ends up as a double in Meta.java

        final Map<String,String> typeLong = new HashMap<String,String>();
        typeLong.put("type", "long");

        // These are the various fields that are possible in metrics.
        // None of the files contain all of them.  But LoadCSV
        // doesn't mind if you specify mappings for fields that don't exist.
        METRIC_TYPE_MAPPINGS.put("t", typeLong);
        METRIC_TYPE_MAPPINGS.put("count", typeLong);
        METRIC_TYPE_MAPPINGS.put("value", typeFloat);
        METRIC_TYPE_MAPPINGS.put("max", typeFloat);
        METRIC_TYPE_MAPPINGS.put("mean", typeFloat);
        METRIC_TYPE_MAPPINGS.put("min", typeFloat);
        METRIC_TYPE_MAPPINGS.put("mean_rate", typeFloat);
        METRIC_TYPE_MAPPINGS.put("m1_rate", typeFloat);
        METRIC_TYPE_MAPPINGS.put("m5_rate", typeFloat);
        METRIC_TYPE_MAPPINGS.put("m15_rate", typeFloat);
        METRIC_TYPE_MAPPINGS.put("p50", typeFloat);
        METRIC_TYPE_MAPPINGS.put("p75", typeFloat);
        METRIC_TYPE_MAPPINGS.put("p95", typeFloat);
        METRIC_TYPE_MAPPINGS.put("p98", typeFloat);
        METRIC_TYPE_MAPPINGS.put("p99", typeFloat);
        METRIC_TYPE_MAPPINGS.put("p999", typeFloat);
    }
}
