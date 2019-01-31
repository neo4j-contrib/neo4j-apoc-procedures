package apoc.metrics;

import apoc.ApocConfiguration;
import apoc.load.LoadCsv;
import apoc.util.FileUtils;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Stream;

public class Metrics {
    /**
     * DAO for a single line in a metrics/*.csv file.
     * Value is abstracted to double; in reality some are int, some are float, some are double.
     */
    public class Neo4jMetricEntry {
        public long t;
        public double value;
        public String metric;

        public Neo4jMetricEntry(long t, double value, String metric) {
            this.t = t;
            this.value = value;
            this.metric = metric;
        }
    }

    public class Neo4jMetric {
        public String name;
        public long lastUpdated;
        public String path;

        public Neo4jMetric(String name, long lastUpdated, String path) {
            this.name = name;
            this.lastUpdated = lastUpdated;
            this.path = path;
        }
    }

    public class Neo4jMetricConfiguration {
        public String metricType;
        public Object enabled;

        public Neo4jMetricConfiguration(String metricType, Object enabled) {
            this.metricType = metricType;
            this.enabled = enabled;
        }
    }

    // TODO: Do we want this?  It only returns what's in the conf file, not the effective configuration.
    // So if you don't set metrics.enabled=true, it **is** true, but the value comes back as null.
    @Procedure
    @Description("apoc.metrics.configuration() - get configured metric types")
    public Stream<Neo4jMetricConfiguration> configuration() {
        ArrayList<Neo4jMetricConfiguration> l = new ArrayList<Neo4jMetricConfiguration>();

        l.add(new Neo4jMetricConfiguration("all", ApocConfiguration.allConfig.get("metrics.enabled")));
        l.add(new Neo4jMetricConfiguration("neo4j", ApocConfiguration.allConfig.get("metrics.neo4j.enabled")));
        l.add(new Neo4jMetricConfiguration("tx", ApocConfiguration.allConfig.get("metrics.neo4j.tx.enabled")));
        l.add(new Neo4jMetricConfiguration("pagecache", ApocConfiguration.allConfig.get("metrics.neo4j.pagecache.enabled")));
        l.add(new Neo4jMetricConfiguration("counts", ApocConfiguration.allConfig.get("metrics.neo4j.counts.enabled")));
        l.add(new Neo4jMetricConfiguration("network", ApocConfiguration.allConfig.get("metrics.neo4j.network.enabled")));

        return l.stream();
    }

    @Procedure
    @Description("apoc.metrics.list() - get a list of available metrics")
    public Stream<Neo4jMetric> list() {
        Object metricsSetting = ApocConfiguration.allConfig.get("dbms.directories.metrics");

        File metricsDir = new File(metricsSetting == null ?
                ApocConfiguration.allConfig.get("unsupported.dbms.directories.neo4j_home") + File.separator + "metrics"
                : metricsSetting.toString());

        System.out.println("Metrics setting " + metricsSetting);
        System.out.println("Derived file" + metricsDir);

        if (!metricsDir.exists() || !metricsDir.isDirectory()) {
            return Stream.empty();
        }

        FileUtils.checkReadAllowed(metricsDir.toURI().toString());

        return Arrays.asList(metricsDir.list())
                .stream()
                .filter(s -> s.endsWith(".csv"))
                .map(metricFile -> {
                    String metricName = metricFile.substring(0, metricFile.length() - 4);
                    File f = new File(metricsDir + File.separator + metricFile);

                    return new Neo4jMetric(metricName, f.lastModified(), f.getAbsolutePath());
                });
    }

    @Procedure
    @Description("apoc.metrics.get(metric_name) - retrieve a system metric")
    /**
     * This method is a specialization of apoc.load.csv, it just happens that the directory and file path
     * are local.  For Neo4j metrics, see: https://neo4j.com/docs/operations-manual/current/monitoring/metrics/reference/
     *
     * The reason it's not suitable for users to use regular apoc.load.csv to get at the metrics file is that from the
     * outside they have no way of knowing the home path where they're located, and apoc.load.csv doesn't allow
     * relative paths.
     */
    public Stream<Neo4jMetricEntry> get(
            @Name("metric_name") String metricName,
            @Name(value = "config",defaultValue = "{}") Map<String, Object> config) {
        // These config parameters are generally true of Neo4j metrics.
        config.putIfAbsent("sep", ",");
        config.putIfAbsent("header", true);

        String neo4jHome = ApocConfiguration.allConfig.get("unsupported.dbms.directories.neo4j_home");
        String url = "file://" + neo4jHome + File.separator + "metrics" + File.separator + metricName + ".csv";

        FileUtils.checkReadAllowed(url);

        return new LoadCsv().csv(url, config).map(entry -> {
            long t = 0;
            double v = 0.0;

            try {
                t = Long.parseLong(entry.map.get("t").toString());
            } catch (NumberFormatException exc) {
                // Load CSV will give you the header row.
                return null;
            }

            try {
                v = Double.parseDouble(entry.map.get("value").toString());
            } catch(NumberFormatException exc) {
                return null;
            }

            return new Neo4jMetricEntry(t, v, metricName);
        }).filter(p -> p != null);
    }
}
