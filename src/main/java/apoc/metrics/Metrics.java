package apoc.metrics;

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
        public final long timestamp;
        public final double value;
        public final String metric;

        public Neo4jMetricEntry(long t, double value, String metric) {
            this.timestamp = t;
            this.value = value;
            this.metric = metric;
        }
    }

    /**
     * Metrics named neo4j.causal_clustering.core.message_processing_timer.*
     * have a different schema:
     * t,count,max,mean,min,stddev,p50,p75,p95,p98,p99,p999,mean_rate,m1_rate,m5_rate,m15_rate,rate_unit,duration_unit
     */
    public static class CausalClusterTimerMetric {
        public final long timestamp;
        public final long count;
        public final double max;
        public final double mean;
        public final double min;
        public final double stddev;
        public final double p50;
        public final double p75;
        public final double p95;
        public final double p98;
        public final double p99;
        public final double p999;
        public final double mean_rate;
        public final double m1_rate;
        public final double m5_rate;
        public final double m15_rate;
        public final String rate_unit;
        public final String duration_unit;

        public CausalClusterTimerMetric(
            long t, long count, double max, double mean,
            double min, double stddev, double p50, double p75, double p95,
            double p98, double p99, double p999, double mean_rate, double m1_rate,
            double m5_rate, double m15_rate, String rate_unit, String duration_unit
        ) {
            this.timestamp = t;
            this.count = count;
            this.max = max;
            this.mean = mean;
            this.min = min;
            this.stddev = stddev;
            this.p50 = p50;
            this.p75 = p75;
            this.p95 = p95;
            this.p98 = p98;
            this.p99 = p99;
            this.p999 = p999;
            this.mean_rate = mean_rate;
            this.m1_rate = m1_rate;
            this.m5_rate = m5_rate;
            this.m15_rate = m15_rate;
            this.rate_unit = rate_unit;
            this.duration_unit = duration_unit;
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

    @Procedure(mode=Mode.DBMS)
    @Description("apoc.metrics.list() - get a list of available metrics")
    public Stream<Neo4jMetric> list() {
        File metricsDir = FileUtils.getMetricsDirectory();

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

    public Stream<LoadCsv.CSVResult> loadCsvForMetric(String metricName, Map<String,Object> config) {
        // These config parameters are generally true of Neo4j metrics.
        config.putIfAbsent("sep", ",");
        config.putIfAbsent("header", true);

        File metricsDir = FileUtils.getMetricsDirectory();

        if (metricsDir == null) {
            throw new RuntimeException("Metrics directory either does not exist or is not readable.  " +
                    "To use this procedure please ensure CSV metrics are configured " +
                    "https://neo4j.com/docs/operations-manual/current/monitoring/metrics/expose/#metrics-csv");
        }

        String url = new File(metricsDir, metricName + ".csv").toURI().toString();
        return new LoadCsv().csv(url, config);
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

        if (metricName != null && metricName.contains("neo4j.causal_clustering.core.message_processing_timer")) {
            throw new RuntimeException("This procedure does not support causal clustering message processing timer metrics. " +
                    "Please use apoc.metrics.getClusterTimerMetric");
        }

        return loadCsvForMetric(metricName, config).map(entry -> {
            try {
                long t = longFrom(entry, "t");
                double v = doubleFrom(entry, "value");
                return new Neo4jMetricEntry(t, v, metricName);
            } catch (NumberFormatException exc) {
                return null;
            }
        }).filter(p -> p != null);
    }

    @Procedure(mode=Mode.DBMS)
    @Description("apoc.metrics.getClusterTimerMetric(metricName, {}) - retrieve a system metric by its metric name. " +
            "This procedure is only intended for neo4j.causal_clustering.core.message_processing_timer.* metrics. " +
            "Additional configuration options may be passed matching the options available for apoc.load.csv.")
    public Stream<CausalClusterTimerMetric> getClusterTimerMetric(
            @Name("metricName") String metricName,
            @Name(value = "config", defaultValue="{}") Map<String,Object> config) {
        if (metricName == null || !metricName.startsWith("neo4j.causal_clustering.core.message_processing_timer")) {
            throw new RuntimeException("This procedure supports only neo4j.causal_clustering.core.message_processing_timer.* metrics" +
                    " - for all other metrics please use apoc.metrics.get");
        }

        return loadCsvForMetric(metricName, config).map(entry -> {
            try {
                long timestamp = longFrom(entry, "t");
                long count = longFrom(entry, "count");
                double max = doubleFrom(entry, "max");
                double mean = doubleFrom(entry, "mean");
                double min = doubleFrom(entry, "min");
                double stddev = doubleFrom(entry, "stddev");
                double p50 = doubleFrom(entry, "p50");
                double p75 = doubleFrom(entry, "p75");
                double p95 = doubleFrom(entry, "p95");
                double p98 = doubleFrom(entry, "p98");
                double p99 = doubleFrom(entry, "p99");
                double p999 = doubleFrom(entry, "p999");
                double mean_rate = doubleFrom(entry, "mean_rate");
                double m1_rate = doubleFrom(entry, "m1_rate");
                double m5_rate = doubleFrom(entry, "m5_rate");
                double m15_rate = doubleFrom(entry, "m15_rate");
                String rate_unit = entry.map.get("rate_unit").toString();
                String duration_unit = entry.map.get("duration_unit").toString();

                return new CausalClusterTimerMetric(timestamp, count,
                        max, mean, min, stddev, p50, p75, p95, p98, p99, p999, mean_rate,
                        m1_rate, m5_rate, m15_rate, rate_unit, duration_unit);
            } catch (NumberFormatException exc) {
                return null;
            }
        }).filter(p -> p != null);
    }

    private final double doubleFrom(LoadCsv.CSVResult r, String column) {
        return Double.valueOf(r.map.get(column).toString());
    }

    private final long longFrom(LoadCsv.CSVResult r, String column) {
        return Long.parseLong(r.map.get(column).toString());
    }
}
