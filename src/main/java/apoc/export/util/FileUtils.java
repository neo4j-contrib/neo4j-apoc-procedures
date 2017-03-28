package apoc.export.util;

import apoc.ApocConfiguration;
import apoc.util.Util;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

/**
 * @author mh
 * @since 22.05.16
 */
public class FileUtils {

    public static CountingReader readerFor(String fileName) throws IOException {
        checkReadAllowed(fileName);
        if (fileName==null) return null;
        fileName= changeFileUrlIfImportDirectoryConstrained(fileName);
        if (fileName.matches("^\\w+:/.+")) {
            return Util.openInputStream(fileName,null,null).asReader();
        }
        File file = new File(fileName);
        if (!file.exists() || !file.isFile() || !file.canRead()) throw new IOException("Cannot open file "+fileName+" for reading.");
        return new CountingReader(file);
    }

    public static String changeFileUrlIfImportDirectoryConstrained(String url) throws IOException {
        if (isFile(url) && ApocConfiguration.isEnabled("import.file.use_neo4j_config")) {
            if (!ApocConfiguration.isEnabled("import.file.allow_read_from_filesystem"))
                throw new RuntimeException("Import file "+url+" not enabled, please set dbms.security.allow_csv_import_from_file_urls=true in your neo4j.conf");
            String importDir = ApocConfiguration.get("import.file.directory", null);
            if (importDir != null && !importDir.isEmpty()) {
                try {
                    File dir = new File(importDir);
                    if (!dir.exists() || !dir.isDirectory() || !dir.canRead()) throw new Exception();
                    String fileAsUrl = null;
                    try { fileAsUrl = new URI(url).getPath(); } catch (Exception e) { /*no protocol*/ }
                    if ("".equals(fileAsUrl)){
                        String newUrl = url.replace("file://", "file:///");
                        fileAsUrl = new URI(newUrl).getPath();
                        if ("".equals(fileAsUrl)) fileAsUrl=newUrl.replace("file://","");
                    }
                    File file = url.startsWith(importDir) ? new File(fileAsUrl) : new File(dir.getAbsolutePath(), fileAsUrl);
                    if (!file.exists() || !file.isFile() || !file.canRead()) throw new Exception();
                    return new URL("file","",file.getAbsolutePath()).toString();
                } catch (Exception e){
                    throw new IOException("Cannot open file "+url+" from directory "+importDir+" for reading.");
                }
            }
        }
        return url;
    }

    public static boolean isFile(String fileName) {
        if (fileName==null) return false;
        if (fileName.toLowerCase().startsWith("http")) return false;
        if (fileName.toLowerCase().startsWith("file:")) return true;
        return true;
    }

    public static PrintWriter getPrintWriter(String fileName, Writer out) throws IOException {
        if (fileName == null) return null;
        Writer writer = fileName.equals("-") ? out : new BufferedWriter(new FileWriter(fileName));
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
}
