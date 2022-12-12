package apoc.util;

import apoc.ApocConfig;
import apoc.export.util.CountingInputStream;
import apoc.export.util.CountingReader;
import apoc.export.util.ExportConfig;
import apoc.util.hdfs.HDFSUtils;
import apoc.util.s3.S3URLConnection;
import apoc.util.s3.S3UploadUtils;
import org.apache.commons.io.FilenameUtils;
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
import java.util.Optional;

import static apoc.ApocConfig.APOC_IMPORT_FILE_ALLOW__READ__FROM__FILESYSTEM;
import static apoc.ApocConfig.apocConfig;
import static apoc.util.Util.ERROR_BYTES_OR_STRING;
import static apoc.util.Util.REDIRECT_LIMIT;
import static apoc.util.Util.readHttpInputStream;

/**
 * @author mh
 * @since 22.05.16
 */
public class FileUtils {

    public enum SupportedProtocols {
        http(true, null),
        https(true, null),
        ftp(true, null),
        s3(Util.classExists("com.amazonaws.services.s3.AmazonS3"),
                "apoc.util.s3.S3UrlStreamHandlerFactory"),
        gs(Util.classExists("com.google.cloud.storage.Storage"),
                "apoc.util.google.cloud.GCStorageURLStreamHandlerFactory"),
        hdfs(Util.classExists("org.apache.hadoop.fs.FileSystem"),
                "org.apache.hadoop.fs.FsUrlStreamHandlerFactory"),
        file(true, null);

        private final boolean enabled;

        private final String urlStreamHandlerClassName;

        SupportedProtocols(boolean enabled, String urlStreamHandlerClassName) {
            this.enabled = enabled;
            this.urlStreamHandlerClassName = urlStreamHandlerClassName;
        }

        public StreamConnection getStreamConnection(String urlAddress, Map<String, Object> headers, String payload) throws IOException {
            switch (this) {
                case s3:
                    return FileUtils.openS3InputStream(urlAddress);
                case hdfs:
                    return FileUtils.openHdfsInputStream(urlAddress);
                case ftp:
                case http:
                case https:
                case gs:
                    return readHttpInputStream(urlAddress, headers, payload, REDIRECT_LIMIT);
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

        public OutputStream getOutputStream(String fileName, ExportConfig config) {
            if (fileName == null) return null;
            final CompressionAlgo compressionAlgo = CompressionAlgo.valueOf(config.getCompressionAlgo());
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
                return new BufferedOutputStream(compressionAlgo.getOutputStream(outputStream));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        public boolean isEnabled() {
            return enabled;
        }

        public URLStreamHandler createURLStreamHandler() {
            return Optional.ofNullable(urlStreamHandlerClassName)
                    .map(Util::createInstanceOrNull)
                    .map(urlStreamHandlerFactory -> ((URLStreamHandlerFactory) urlStreamHandlerFactory).createURLStreamHandler(this.name()))
                    .orElse(null);
        }

        public static SupportedProtocols from(String source) {
            try {
                final URL url = new URL(source);
                return from(url);
            } catch (MalformedURLException e) {
                if (!e.getMessage().contains("no protocol")) {
                    try {
                        // in case new URL(source) throw e.g. unknown protocol: hdfs, because of missing jar, 
                        // we retrieve the related enum and throw the associated MissingDependencyException(..)
                        // otherwise we return unknown protocol: yyyyy
                        return SupportedProtocols.valueOf(new URI(source).getScheme());
                    } catch (Exception ignored) {}
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

    public static CountingReader readerFor(Object input) throws IOException {
        return readerFor(input, null, null, CompressionAlgo.NONE.name());
    }

    public static CountingReader readerFor(Object input, String compressionAlgo) throws IOException {
        return readerFor(input, null, null, compressionAlgo);
    }

    public static CountingReader readerFor(Object input, Map<String, Object> headers, String payload, String compressionAlgo) throws IOException {
        return inputStreamFor(input, headers, payload, compressionAlgo).asReader();
    }

    public static CountingInputStream inputStreamFor(Object input, Map<String, Object> headers, String payload, String compressionAlgo) throws IOException {
        if (input == null) return null;
        if (input instanceof String) {
            String fileName = (String) input;
            apocConfig().checkReadAllowed(fileName);
            fileName = changeFileUrlIfImportDirectoryConstrained(fileName);
            return Util.openInputStream(fileName, headers, payload, compressionAlgo);
        } else if (input instanceof byte[]) {
            return getInputStreamFromBinary((byte[]) input, compressionAlgo);
        } else {
            throw new RuntimeException(ERROR_BYTES_OR_STRING);
        }
    }
    
    public static String changeFileUrlIfImportDirectoryConstrained(String url) throws IOException {
        if (isFile(url) && isImportUsingNeo4jConfig()) {
            if (!apocConfig().getBoolean(APOC_IMPORT_FILE_ALLOW__READ__FROM__FILESYSTEM)) {
                throw new RuntimeException(String.format(ERROR_READ_FROM_FS_NOT_ALLOWED, url));
            }
            final Path resolvedPath = resolvePath(url);
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
        if (FilenameUtils.getPrefixLength(urlPath.toString()) > 0 && !urlPath.startsWith(basePath.toAbsolutePath())) {
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
            final URI uri = URI.create(url.trim()).normalize();
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
            return resolvedPath.toFile().getCanonicalFile().toPath().startsWith(basePath.toRealPath());
        } catch (Exception e) {
            if (e instanceof NoSuchFileException) { // If we're about to create a file this exception has been thrown
                return resolvedPath.toFile().getCanonicalFile().toPath().startsWith(basePath);
            }
            return false;
        }
    }

    public static boolean isFile(String fileName) {
        return SupportedProtocols.from(fileName) == SupportedProtocols.file;
    }

    public static OutputStream getOutputStream(String fileName) {
        return getOutputStream(fileName, ExportConfig.EMPTY);
    }

    public static OutputStream getOutputStream(String fileName, ExportConfig config) {
        if (fileName.equals("-")) {
            return null;
        }
        return SupportedProtocols.from(fileName).getOutputStream(fileName, config);
    }

    public static boolean isImportUsingNeo4jConfig() {
        return apocConfig().getBoolean(ApocConfig.APOC_IMPORT_FILE_USE_NEO4J_CONFIG);
    }

    public static StreamConnection openS3InputStream(String urlAddress) throws IOException {
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
        return S3URLConnection.openS3InputStream(new URL(urlAddress));
    }

    public static StreamConnection openHdfsInputStream(String urlAddress) throws IOException {
        if (!SupportedProtocols.hdfs.isEnabled()) {
            throw new MissingDependencyException("Cannot find the HDFS/Hadoop jars in the plugins folder. \n" +
                    "\nPlease, see the documentation: https://neo4j.com/labs/apoc/4.4/import/web-apis/");
        }
        return HDFSUtils.readFile(new URL(urlAddress));
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

    public static Path getPathFromUrlString(String urlDir) {
        return Paths.get(URI.create(urlDir));
    }

    public static CountingInputStream getInputStreamFromBinary(byte[] urlOrBinary, String compressionAlgo) {
        return CompressionAlgo.valueOf(compressionAlgo).toInputStream(urlOrBinary);
    }

    public static CountingReader getReaderFromBinary(byte[] urlOrBinary, String compressionAlgo) {
        try {
            return getInputStreamFromBinary(urlOrBinary, compressionAlgo).asReader();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
