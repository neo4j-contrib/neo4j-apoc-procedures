package apoc.export.util;

import java.io.*;
import java.net.URL;
import java.net.URLConnection;
/**
 * @author mh
 * @since 22.05.16
 */
public class FileUtils {

    public static CountingReader readerFor(String fileName) throws IOException {
        if (fileName==null) return null;
        if (fileName.startsWith("http") || fileName.startsWith("file:")) {
            URL url = new URL(fileName);
            URLConnection conn = url.openConnection();
            long size = conn.getContentLengthLong();
            Reader reader = new InputStreamReader(url.openStream(),"UTF-8");
            return new CountingReader(reader,size);
        }
        File file = new File(fileName);
        if (!file.exists() || !file.isFile() || !file.canRead()) throw new IOException("Cannot open file "+fileName+" for reading.");
        return new CountingReader(file);
    }

    public static PrintWriter getPrintWriter(String fileName, Writer out) throws IOException {
        if (fileName == null) return null;
        Writer writer = fileName.equals("-") ? out : new BufferedWriter(new FileWriter(fileName));
        return new PrintWriter(writer);
    }

}
