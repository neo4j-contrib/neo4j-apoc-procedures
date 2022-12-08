package apoc.util;

import static apoc.ApocConfig.apocConfig;

import apoc.export.util.CountingReader;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import org.neo4j.configuration.GraphDatabaseSettings;

public class ExtendedFileUtils
{
    /**
     * @return a File representing the metrics directory that is listable and readable, or null if metrics don't exist,
     * aren't enabled, or aren't readable.
     */
    public static File getMetricsDirectory() {
        String neo4jHome = apocConfig().getString( GraphDatabaseSettings.neo4j_home.name());
        String metricsSetting = apocConfig().getString("server.directories.metrics", neo4jHome + File.separator + "metrics");

        File metricsDir = metricsSetting.isEmpty() ? new File(neo4jHome, "metrics") : new File(metricsSetting);

        if (metricsDir.exists() && metricsDir.canRead() && metricsDir.isDirectory() ) {
            return metricsDir;
        }

        return null;
    }

    public static Path getPathFromUrlString(String urlDir) {
        return Paths.get( URI.create(urlDir));
    }

    // This is the list of dbms.directories.* valid configuration items for neo4j.
    // https://neo4j.com/docs/operations-manual/current/reference/configuration-settings/
    // Usually these reside under the same root but because they're separately configurable, in the worst case
    // every one is on a different device.
    //
    // More likely, they'll be largely similar metrics.
    public static final List<String> NEO4J_DIRECTORY_CONFIGURATION_SETTING_NAMES = Arrays.asList(
//            "dbms.directories.certificates",  // not in 4.x version
            "server.directories.data",
            "server.directories.import",
            "server.directories.lib",
            "server.directories.logs",
//            "server.directories.metrics",  // metrics is only in EE
            "server.directories.plugins",
            "server.directories.run",
            "server.directories.transaction.logs.root", // in Neo4j 5.0 GraphDatabaseSettings.transaction_logs_root_path changed from tx_log to this config
            "server.directories.neo4j_home"
    );

    public static void closeReaderSafely( CountingReader reader) {
        if (reader != null) {
            try { reader.close(); } catch ( IOException ignored) { }
        }
    }
}
