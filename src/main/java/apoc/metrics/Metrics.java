package apoc.metrics;

import apoc.ApocConfiguration;
import apoc.load.LoadCsv;
import apoc.util.FileUtils;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import java.io.File;
import java.io.FilenameFilter;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Stream;

public class Metrics {
    /**
     * DAO for a single line in a metrics/*.csv file.
     * Value is abstracted to double; in reality some are int, some are float, some are double.
     */
    public static class Neo4jMetricEntry {
        public final long t;
        public final double value;
        public final String metric;

        public Neo4jMetricEntry(long t, double value, String metric) {
            this.t = t;
            this.value = value;
            this.metric = metric;
        }
    }

    public static class Neo4jMetric {
        public final String name;
        public final long lastUpdated;

        public Neo4jMetric(String name, long lastUpdated) {
            this.name = name;
            this.lastUpdated = lastUpdated;
        }
    }

    @Procedure
    @Description("apoc.metrics.list() - get a list of available metrics")
    public Stream<Neo4jMetric> list() {
        Object metricsSetting = ApocConfiguration.get("dbms.directories.metrics", "");

        File metricsDir = (
                (metricsSetting == null) ?
                        new File(ApocConfiguration.get("unsupported.dbms.directories.neo4j_home", ""), "metrics") :
                        new File(metricsSetting.toString()));

        if (!metricsDir.exists() || !metricsDir.isDirectory() || !metricsDir.canRead()) {
            return Stream.empty();
        }

        FileUtils.checkReadAllowed(metricsDir.toURI().toString());

        final FilenameFilter filter = (dir, name) -> name.toLowerCase().endsWith(".csv");
        return Arrays.asList(metricsDir.listFiles(filter))
                .stream()
                .map(metricFile -> {
                    String name = metricFile.getName();
                    String metricName = name.substring(0, name.length() - 4);
                    File f = new File(metricsDir, name);
                    return new Neo4jMetric(metricName, f.lastModified());
                });
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
    public Stream<Neo4jMetricEntry> get(
            @Name("metricName") String metricName,
            @Name(value = "config",defaultValue = "{}") Map<String, Object> config) {
        // These config parameters are generally true of Neo4j metrics.
        config.putIfAbsent("sep", ",");
        config.putIfAbsent("header", true);

        String neo4jHome = ApocConfiguration.get("unsupported.dbms.directories.neo4j_home", "");
        String url = new File(neo4jHome + File.separator + "metrics", metricName + ".csv")
                .toURI().toString();

        FileUtils.checkReadAllowed(url);

        return new LoadCsv().csv(url, config).map(entry -> {
            long t = 0;
            double v = 0.0;

            try {
                t = Long.parseLong(entry.map.get("t").toString());
                v = Double.parseDouble(entry.map.get("value").toString());
            } catch (NumberFormatException exc) {
                return null;
            }

            return new Neo4jMetricEntry(t, v, metricName);
        }).filter(p -> p != null);
    }
}
