package apoc.export.util;

import apoc.ApocConfiguration;
import apoc.util.Util;

import java.io.*;
import java.net.URL;
import java.net.URLConnection;
/**
 * @author mh
 * @since 22.05.16
 */
public class FileUtils {

    public static CountingReader readerFor(String fileName) throws IOException {
        checkReadAllowed(fileName);
        if (fileName==null) return null;
        if (fileName.toLowerCase().startsWith("http") || fileName.toLowerCase().startsWith("file:")) {
            return Util.openInputStream(fileName,null,null).asReader();
        }
        File file = new File(fileName);
        if (!file.exists() || !file.isFile() || !file.canRead()) throw new IOException("Cannot open file "+fileName+" for reading.");
        return new CountingReader(file);
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
