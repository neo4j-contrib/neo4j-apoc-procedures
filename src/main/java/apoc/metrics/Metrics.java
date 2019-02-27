package apoc.metrics;

import apoc.ApocConfiguration;
import apoc.load.LoadCsv;
import apoc.util.FileUtils;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.io.File;
import java.io.FilenameFilter;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Stream;

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

            String configLocation = ApocConfiguration.get(dir, null);
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


        /**
         * Neo4j CSV metrics have an issue where sometimes in the middle of the CSV file you'll find an extra
         * header row.  We want to discard those.
         */
        Predicate duplicatedHeaderRows = new Predicate <LoadCsv.CSVResult> () {
            public boolean test(LoadCsv.CSVResult o) {
                Map<String,Object> map = o.map;

                // Most commonly CSV has a timestamp "t" field, if its value = "t"
                // Then it's a repeated header.  This is just a shortcut for the most common
                // case to avoid checking entire map every time.
                if (map.containsKey("t") && "t".equals(map.get("t").toString())) {
                    return false;
                } else {
                    Set<String> keys = map.keySet();
                    for (String key : keys) {
                        Object val = map.get(key);
                        // If any key (the column header) **does not** match the value in the map,
                        // then it's actual data and not a header row.
                        if (val == null || !key.equals(val.toString())) {
                            return true;
                        }
                    }
                }

                return false;
            }
        };

        String url = new File(metricsDir, metricName + ".csv").toURI().toString();
        return new LoadCsv().csv(url, config)
                .filter(csvResult -> duplicatedHeaderRows.test(csvResult))
                .map(csvResult -> new GenericMetric(metricName, longFrom(csvResult, "t"), csvResult.map));
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
        return loadCsvForMetric(metricName, config);
    }

    private final long longFrom(LoadCsv.CSVResult r, String column) {
        Object o = r.map.get(column);
        if (o == null) return 0L;
        return Long.parseLong(o.toString());
    }
}
