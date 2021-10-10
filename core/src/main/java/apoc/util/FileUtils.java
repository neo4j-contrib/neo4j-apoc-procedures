package apoc.util;

import apoc.ApocConfig;
import apoc.export.util.CountingInputStream;
import apoc.export.util.CountingReader;
import apoc.util.hdfs.HDFSUtils;
import apoc.util.s3.S3URLConnection;
import apoc.util.s3.S3UploadUtils;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.configuration.GraphDatabaseSettings;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.URLStreamHandler;
import java.net.URLStreamHandlerFactory;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static apoc.ApocConfig.APOC_IMPORT_FILE_ALLOW__READ__FROM__FILESYSTEM;
import static apoc.ApocConfig.apocConfig;
import static apoc.util.Util.readHttpInputStream;
import static org.eclipse.jetty.util.URIUtil.encodePath;

/**
 * @author mh
 * @since 22.05.16
 */
public class FileUtils {

    public enum SupportedProtocols {
        http(true, null),
        https(true, null),
        s3(Util.classExists("com.amazonaws.services.s3.AmazonS3"),
                Util.createInstanceOrNull("apoc.util.s3.S3UrlStreamHandlerFactory")),
        gs(Util.classExists("com.google.cloud.storage.Storage"),
                Util.createInstanceOrNull("apoc.util.google.cloud.GCStorageURLStreamHandlerFactory")),
        hdfs(Util.classExists("org.apache.hadoop.fs.FileSystem"),
                Util.createInstanceOrNull("org.apache.hadoop.fs.FsUrlStreamHandlerFactory")),
        file(true, null);

        private final boolean enabled;

        private final URLStreamHandlerFactory urlStreamHandlerFactory;

        SupportedProtocols(boolean enabled, URLStreamHandlerFactory urlStreamHandlerFactory) {
            this.enabled = enabled;
            this.urlStreamHandlerFactory = urlStreamHandlerFactory;
        }

        public StreamConnection getStreamConnection(String urlAddress, Map<String, Object> headers, String payload) throws IOException {
            switch (this) {
                case s3:
                    return FileUtils.openS3InputStream(new URL(urlAddress));
                case hdfs:
                    return FileUtils.openHdfsInputStream(new URL(urlAddress));
                case http:
                case https:
                case gs:
                    return readHttpInputStream(urlAddress, headers, payload);
                default:
                    try {
                        return new StreamConnection.FileStreamConnection(URI.create(urlAddress));
                    } catch (IllegalArgumentException iae) {
                        try {
                            return new StreamConnection.FileStreamConnection(new URL(urlAddress).getFile());
                        } catch (MalformedURLException mue) {
                            if (mue.getMessage().contains("no protocol")) {
                                return new StreamConnection.FileStreamConnection(urlAddress);
                            }
                            throw mue;
                        }
                    }
            }
        }

        public OutputStream getOutputStream(String fileName) {
            if (fileName == null) return null;
            final OutputStream outputStream;
            try {
                switch (this) {
                    case s3:
                        outputStream = S3UploadUtils.writeFile(fileName);
                        break;
                    case hdfs:
                        outputStream = HDFSUtils.writeFile(fileName);
                        break;
                    default:
                        final Path path = resolvePath(fileName);
                        outputStream = new FileOutputStream(path.toFile());
                }
                return new BufferedOutputStream(outputStream);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public boolean isEnabled() {
            return enabled;
        }

        public URLStreamHandler createURLStreamHandler() {
            return urlStreamHandlerFactory == null ? null : urlStreamHandlerFactory.createURLStreamHandler(this.name());
        }

        public static SupportedProtocols from(String source) {
            try {
                final URL url = new URL(source);
                return from(url);
            } catch (MalformedURLException e) {
                if (!e.getMessage().contains("no protocol")) {
                    throw new RuntimeException(e);
                }
                return SupportedProtocols.file;
            }
        }

        public static SupportedProtocols from(URL url) {
            return SupportedProtocols.of(url.getProtocol());
        }

        public static SupportedProtocols of(String name) {
            try {
                return SupportedProtocols.valueOf(name);
            } catch (Exception e) {
                return file;
            }
        }

    }

    public static final String ERROR_READ_FROM_FS_NOT_ALLOWED = "Import file %s not enabled, please set " + APOC_IMPORT_FILE_ALLOW__READ__FROM__FILESYSTEM + "=true in your neo4j.conf";
    public static final String ACCESS_OUTSIDE_DIR_ERROR = "You're providing a directory outside the import directory " +
            "defined into `dbms.directories.import`";

    public static CountingReader readerFor(String fileName) throws IOException {
        return readerFor(fileName, null, null);
    }

    public static CountingReader readerFor(String fileName, Map<String, Object> headers, String payload) throws IOException {
        return inputStreamFor(fileName, headers, payload).asReader();
    }

    public static CountingInputStream inputStreamFor(String fileName, Map<String, Object> headers, String payload) throws IOException {
        apocConfig().checkReadAllowed(fileName);
        if (fileName == null) return null;
        fileName = changeFileUrlIfImportDirectoryConstrained(fileName);
        return Util.openInputStream(fileName,headers,payload);
    }

    public static CountingInputStream inputStreamFor(String fileName) throws IOException {
        return inputStreamFor(fileName, null, null);
    }

    public static String changeFileUrlIfImportDirectoryConstrained(String urlNotEncoded) throws IOException {
        final String url = encodeExceptQM(urlNotEncoded);
        if (isFile(url) && isImportUsingNeo4jConfig()) {
            if (!apocConfig().getBoolean(APOC_IMPORT_FILE_ALLOW__READ__FROM__FILESYSTEM)) {
                throw new RuntimeException(String.format(ERROR_READ_FROM_FS_NOT_ALLOWED, url));
            }
            final Path resolvedPath = resolvePath(urlNotEncoded);
            return resolvedPath
                    .normalize()
                    .toUri()
                    .toString();
        }
        return url;
    }

    private static Path resolvePath(String url) throws IOException {
        Path urlPath = getPath(url);
        final Path resolvedPath;
        if (apocConfig().isImportFolderConfigured() && isImportUsingNeo4jConfig()) {
            Path basePath = Paths.get(apocConfig().getImportDir());
            urlPath = relativizeIfSamePrefix(urlPath, basePath);
            resolvedPath = basePath.resolve(urlPath).toAbsolutePath().normalize();
            if (!pathStartsWithOther(resolvedPath, basePath)) {
                throw new IOException(ACCESS_OUTSIDE_DIR_ERROR);
            }
        } else {
            resolvedPath = urlPath;
        }
        return resolvedPath;
    }

    private static Path relativizeIfSamePrefix(Path urlPath, Path basePath) {
        if (urlPath.isAbsolute() && !urlPath.startsWith(basePath.toAbsolutePath())) {
            // if the import folder is configured to be used as root folder we consider
            // it as root directory in order to reproduce the same LOAD CSV behaviour
            urlPath = urlPath.getRoot().relativize(urlPath);
        }
        return urlPath;
    }

    private static Path getPath(String url) {
        Path urlPath;
        URL toURL = null;
        try {
            final URI uri = URI.create(url.trim());
            toURL = uri.toURL();
            urlPath = Paths.get(uri);
        } catch (Exception e) {
            if (toURL != null) {
                urlPath = Paths.get(StringUtils.isBlank(toURL.getFile()) ? toURL.getHost() : toURL.getFile());
            } else {
                urlPath = Paths.get(url);
            }
        }
        return urlPath;
    }

    private static boolean pathStartsWithOther(Path resolvedPath, Path basePath) throws IOException {
        try {
            return resolvedPath.toRealPath().startsWith(basePath.toRealPath());
        } catch (Exception e) {
            if (e instanceof NoSuchFileException) { // If we're about to creating a file this exception has been thrown
                return resolvedPath.normalize().startsWith(basePath);
            }
            return false;
        }
    }

    public static boolean isFile(String fileName) {
        return SupportedProtocols.from(fileName) == SupportedProtocols.file;
    }

    public static OutputStream getOutputStream(String fileName) {
        if (fileName.equals("-")) {
            return null;
        }
        return SupportedProtocols.from(fileName).getOutputStream(fileName);
    }

    public static boolean isImportUsingNeo4jConfig() {
        return apocConfig().getBoolean(ApocConfig.APOC_IMPORT_FILE_USE_NEO4J_CONFIG);
    }

    public static StreamConnection openS3InputStream(URL url) throws IOException {
        if (!SupportedProtocols.s3.isEnabled()) {
            throw new MissingDependencyException("Cannot find the S3 jars in the plugins folder. \n" +
                    "Please put these files into the plugins folder :\n\n" +
                    "aws-java-sdk-core-x.y.z.jar\n" +
                    "aws-java-sdk-s3-x.y.z.jar\n" +
                    "httpclient-x.y.z.jar\n" +
                    "httpcore-x.y.z.jar\n" +
                    "joda-time-x.y.z.jar\n" +
                    "\nSee the documentation: https://neo4j-contrib.github.io/neo4j-apoc-procedures/#_loading_data_from_web_apis_json_xml_csv");
        }
        return S3URLConnection.openS3InputStream(url);
    }

    public static StreamConnection openHdfsInputStream(URL url) throws IOException {
        if (!SupportedProtocols.hdfs.isEnabled()) {
            throw new MissingDependencyException("Cannot find the HDFS/Hadoop jars in the plugins folder. \n" +
                    "Please put these files into the plugins folder :\n\n" +
                    "commons-cli\n" +
                    "hadoop-auth\n" +
                    "hadoop-client\n" +
                    "hadoop-common\n" +
                    "hadoop-hdfs\n" +
                    "htrace-core-3.1.0-incubating\n" +
                    "protobuf-java\n" +
                    "\nSee the documentation: https://neo4j-contrib.github.io/neo4j-apoc-procedures/#_loading_data_from_web_apis_json_xml_csv");
        }
        return HDFSUtils.readFile(url);
    }

    /**
     * @return a File pointing to Neo4j's log directory, if it exists and is readable, null otherwise.
     */
    public static File getLogDirectory() {
        String neo4jHome = apocConfig().getString("dbms.directories.neo4j_home", "");
        String logDir = apocConfig().getString("dbms.directories.logs", "");

        File logs = logDir.isEmpty() ? new File(neo4jHome, "logs") : new File(logDir);

        if (logs.exists() && logs.canRead() && logs.isDirectory()) {
            return logs;
        }

        return null;
    }

    /**
     * @return a File representing the metrics directory that is listable and readable, or null if metrics don't exist,
     * aren't enabled, or aren't readable.
     */
    public static File getMetricsDirectory() {
        String neo4jHome = apocConfig().getString(GraphDatabaseSettings.neo4j_home.name());
        String metricsSetting = apocConfig().getString("dbms.directories.metrics", neo4jHome + File.separator + "metrics");

        File metricsDir = metricsSetting.isEmpty() ? new File(neo4jHome, "metrics") : new File(metricsSetting);

        if (metricsDir.exists() && metricsDir.canRead() && metricsDir.isDirectory() ) {
            return metricsDir;
        }

        return null;
    }

    // This is the list of dbms.directories.* valid configuration items for neo4j.
    // https://neo4j.com/docs/operations-manual/current/reference/configuration-settings/
    // Usually these reside under the same root but because they're separately configurable, in the worst case
    // every one is on a different device.
    //
    // More likely, they'll be largely similar metrics.
    public static final List<String> NEO4J_DIRECTORY_CONFIGURATION_SETTING_NAMES = Arrays.asList(
//            "dbms.directories.certificates",  // not in 4.x version
            "dbms.directories.data",
            "dbms.directories.import",
            "dbms.directories.lib",
            "dbms.directories.logs",
//            "dbms.directories.metrics",  // metrics is only in EE
            "dbms.directories.plugins",
            "dbms.directories.run",
            "dbms.directories.tx_log",
            "dbms.directories.neo4j_home"
    );

    public static void closeReaderSafely(CountingReader reader) {
        if (reader != null) {
            try { reader.close(); } catch (IOException ignored) { }
        }
    }

    public static String getDirImport() {
        return apocConfig().getString("dbms.directories.import", "import");
    }

    public static Path getPathFromUrlString(String urlDir) {
        return Paths.get(URI.create(urlDir).getPath());
    }

    // to exclude cases like 'testload.tar.gz?raw=true'
    private static String encodeExceptQM(String url) {
        return encodePath(url).replace("%3F", "?");
    }
}
