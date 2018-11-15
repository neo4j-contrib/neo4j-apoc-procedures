package apoc.util;

import apoc.ApocConfiguration;
import apoc.export.util.CountingInputStream;
import apoc.export.util.CountingReader;
import apoc.util.hdfs.HDFSUtils;
import apoc.util.s3.S3URLConnection;

import java.io.*;
import java.net.URI;
import java.net.URL;
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
        if (isFile(url) && ApocConfiguration.isEnabled("import.file.use_neo4j_config")) {
            if (!ApocConfiguration.isEnabled("import.file.allow_read_from_filesystem"))
                throw new RuntimeException("Import file "+url+" not enabled, please set dbms.security.allow_csv_import_from_file_urls=true in your neo4j.conf");

            String importDir = ApocConfiguration.get("import.file.directory", null);

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
        if (fileName == null) return null;
        Writer writer;

        if (isHdfs(fileName)) {
            try {
                writer = new OutputStreamWriter(HDFSUtils.writeFile(fileName));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        } else {
            writer = fileName.equals("-") ? out : new BufferedWriter(new FileWriter(fileName));
        }

        return new PrintWriter(writer);
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
}