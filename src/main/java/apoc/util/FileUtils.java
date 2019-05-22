package apoc.util;

import apoc.ApocConfiguration;
import apoc.export.util.CountingInputStream;
import apoc.export.util.CountingReader;
import apoc.util.hdfs.HDFSUtils;
import apoc.util.s3.S3URLConnection;
import org.apache.commons.io.output.WriterOutputStream;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author mh
 * @since 22.05.16
 */
public class FileUtils {

    public static final String S3_PROTOCOL = "s3";
    public static final boolean S3_ENABLED = Util.classExists("com.amazonaws.services.s3.AmazonS3");
    public static final String HDFS_PROTOCOL = "hdfs";
    public static final boolean HDFS_ENABLED = Util.classExists("org.apache.hadoop.fs.FileSystem");
    public static final Pattern HDFS_PATTERN = Pattern.compile("^(hdfs:\\/\\/)(?:[^@\\/\\n]+@)?([^\\/\\n]+)");

    public static CountingReader readerFor(String fileName) throws IOException {
        checkReadAllowed(fileName);
        if (fileName==null) return null;
        fileName = changeFileUrlIfImportDirectoryConstrained(fileName);
        if (fileName.matches("^\\w+:/.+")) {
            if (isHdfs(fileName)) {
                return readHdfs(fileName);
            } else {
                return Util.openInputStream(fileName,null,null).asReader();
            }
        }
        return readFile(fileName);
    }
    public static CountingInputStream inputStreamFor(String fileName) throws IOException {
        checkReadAllowed(fileName);
        if (fileName==null) return null;
        fileName = changeFileUrlIfImportDirectoryConstrained(fileName);
        if (fileName.matches("^\\w+:/.+")) {
            if (isHdfs(fileName)) {
                return readHdfsStream(fileName);
            } else {
                return Util.openInputStream(fileName,null,null);
            }
        }
        return readFileStream(fileName);
    }

    private static CountingInputStream readHdfsStream(String fileName) {
        try {
            StreamConnection streamConnection = HDFSUtils.readFile(fileName);
            return new CountingInputStream(streamConnection.getInputStream(), streamConnection.getLength());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static CountingReader readHdfs(String fileName) {
        try {
            StreamConnection streamConnection = HDFSUtils.readFile(fileName);
            Reader reader = new BufferedReader(new InputStreamReader(streamConnection.getInputStream(), "UTF-8"));
            return new CountingReader(reader, streamConnection.getLength());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static CountingReader readFile(String fileName) throws IOException, FileNotFoundException {
        File file = new File(fileName);
        if (!file.exists() || !file.isFile() || !file.canRead()) throw new IOException("Cannot open file "+fileName+" for reading.");
        return new CountingReader(file);
    }

    private static CountingInputStream readFileStream(String fileName) throws IOException, FileNotFoundException {
        File file = new File(fileName);
        if (!file.exists() || !file.isFile() || !file.canRead()) throw new IOException("Cannot open file "+fileName+" for reading.");
        return new CountingInputStream(file);
    }

    public static String changeFileUrlIfImportDirectoryConstrained(String url) throws IOException {
        if (isFile(url) && isImportUsingNeo4jConfig()) {
            if (!ApocConfiguration.isEnabled("import.file.allow_read_from_filesystem"))
                throw new RuntimeException("Import file "+url+" not enabled, please set dbms.security.allow_csv_import_from_file_urls=true in your neo4j.conf");

            String importDir = ApocConfiguration.get("dbms.directories.import", null);

            URI uri = URI.create(url);
            if(uri == null) throw new RuntimeException("Path not valid!");

            if (importDir != null && !importDir.isEmpty()) {
                try {
                    String relativeFilePath = !uri.getPath().isEmpty() ? uri.getPath() : uri.getHost();
                    String absolutePath = url.startsWith(importDir) ? url : new File(importDir, relativeFilePath).getAbsolutePath();

                    return new File(absolutePath).toURI().toString();
                } catch (Exception e){
                    throw new IOException("Cannot open file "+url+" from directory "+importDir+" for reading.");
                }
            } else {
                try {
                    return new File(uri.getPath()).toURI().toString();
                } catch (Exception e) {
                    throw new IOException("Cannot open file "+url+" for reading.");
                }
            }
        }
        return url;
    }

    public static boolean isFile(String fileName) {
        if (fileName==null) return false;
        if (fileName.toLowerCase().startsWith("http")) return false;
        if (isHdfs(fileName)) return false;
        if (fileName.toLowerCase().startsWith("file:")) return true;
        return true;
    }

    public static PrintWriter getPrintWriter(String fileName, Writer out) throws IOException {
        OutputStream outputStream = getOutputStream(fileName, new WriterOutputStream(out));
        return outputStream == null ? null : new PrintWriter(outputStream);
    }

    public static OutputStream getOutputStream(String fileName, OutputStream out) throws IOException {
        if (fileName == null) return null;
        OutputStream outputStream;
        if (isHdfs(fileName)) {
            try {
                outputStream = HDFSUtils.writeFile(fileName);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        } else {
            outputStream = getOrCreateOutputStream(fileName, out);
//            outputStream = fileName.equals("-") ? out : new FileOutputStream(fileName);
        }
        return new BufferedOutputStream(outputStream);
    }

    private static OutputStream getOrCreateOutputStream(String fileName, OutputStream out) throws FileNotFoundException, MalformedURLException {
        OutputStream outputStream;
        if (fileName.equals("-")) {
            outputStream = out;
        } else {
            boolean enabled = isImportUsingNeo4jConfig();
            if (enabled) {
                String importDir = getConfiguredImportDirectory();
                File file = new File(importDir, fileName);
                outputStream = new FileOutputStream(file);
            } else {
                URI uri = URI.create(fileName);
                outputStream = new FileOutputStream(uri.isAbsolute() ? uri.toURL().getFile() : fileName);
            }
        }
        return outputStream;
    }

    private static boolean isImportUsingNeo4jConfig() {
        return ApocConfiguration.isEnabled("import.file.use_neo4j_config");
    }

    public static String getConfiguredImportDirectory() {
        return ApocConfiguration.get("dbms.directories.import", "import");
    }

    public static void checkReadAllowed(String url) {
        if (isFile(url) && !ApocConfiguration.isEnabled("import.file.enabled"))
            throw new RuntimeException("Import from files not enabled, please set apoc.import.file.enabled=true in your neo4j.conf");
    }
    public static void checkWriteAllowed() {
        if (!ApocConfiguration.isEnabled("export.file.enabled"))
            throw new RuntimeException("Export to files not enabled, please set apoc.export.file.enabled=true in your neo4j.conf");
    }

    public static StreamConnection openS3InputStream(URL url) throws IOException {
        if (!S3_ENABLED) {
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
        if (!HDFS_ENABLED) {
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

    public static boolean isHdfs(String fileName) {
        Matcher matcher = HDFS_PATTERN.matcher(fileName);
        return matcher.find();
    }

    /**
     * @returns a File pointing to Neo4j's log directory, if it exists and is readable, null otherwise.
     */
    public static File getLogDirectory() {
        String neo4jHome = ApocConfiguration.get("unsupported.dbms.directories.neo4j_home", "");
        String logDir = ApocConfiguration.get("dbms.directories.logs", "");

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
        String neo4jHome = ApocConfiguration.get("unsupported.dbms.directories.neo4j_home", "");
        String metricsSetting = ApocConfiguration.get("dbms.directories.metrics", "");

        File metricsDir = metricsSetting.isEmpty() ? new File(neo4jHome, "metrics") : new File(metricsSetting);

        if (metricsDir.exists() && metricsDir.canRead() && metricsDir.isDirectory() ) {
            return metricsDir;
        }

        return null;
    }

    /**
     * Given a file, determine whether it resides in neo4j controlled directory or not.  This method takes into account
     * the possibility of symlinks / hardlinks.  Keep in mind Neo4j does not have one root home, but its configured
     * directories may be spread all over the filesystem, so there's no parent.
     * @param f the file to check
     * @return true if the file's actual storage is in the neo4j home directory, false otherwise.  If f is a symlink
     * that resides in a neo4j directory that points somewhere outside, returns false.
     * @throws IOException if the canonical path cannot be determined.
     */
    public static boolean inNeo4jOwnedDirectory(File f) throws IOException {
        String canonicalPath = f.getCanonicalPath();

        for(String dirSetting : NEO4J_DIRECTORY_CONFIGURATION_SETTING_NAMES) {
            String actualDir = ApocConfiguration.get(dirSetting, null);
            if (canonicalPath.contains(actualDir)) {
                return true;
            }
        }

        return false;
    }

    // This is the list of dbms.directories.* valid configuration items for neo4j.
    // https://neo4j.com/docs/operations-manual/current/reference/configuration-settings/
    // Usually these reside under the same root but because they're separately configurable, in the worst case
    // every one is on a different device.
    //
    // More likely, they'll be largely similar metrics.
    public static final List<String> NEO4J_DIRECTORY_CONFIGURATION_SETTING_NAMES = Arrays.asList(
            "dbms.directories.certificates",
            "dbms.directories.data",
            "dbms.directories.import",
            "dbms.directories.lib",
            "dbms.directories.logs",
            "dbms.directories.metrics",
            "dbms.directories.plugins",
            "dbms.directories.run",
            "dbms.directories.tx_log",
            "unsupported.dbms.directories.neo4j_home"
    );
}
