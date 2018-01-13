package apoc.export.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.Writer;
import java.net.URI;
import java.net.URL;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import apoc.ApocConfiguration;
import apoc.util.Util;

/**
 * @author mh
 * @since 22.05.16
 */
public class FileUtils {

    public static CountingReader readerFor(String fileName) throws IOException {
        checkReadAllowed(fileName);
        if (fileName==null) return null;
        fileName = changeFileUrlIfImportDirectoryConstrained(fileName);
        if (fileName.matches("^\\w+:/.+")) {
        	if (HDFSUtils.isHdfs(fileName)) {
        		return readHdfs(fileName);
        	} else {
        		return Util.openInputStream(fileName,null,null).asReader();
        	}
        }
        return readFile(fileName);
    }

	private static CountingReader readHdfs(String fileName) {
		try {
	        InputStream inputStream = HDFSUtils.readFile(fileName);
			Reader reader = new BufferedReader(new InputStreamReader(inputStream, "UTF-8"));
			return new CountingReader(reader, inputStream.available());
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private static CountingReader readFile(String fileName) throws IOException, FileNotFoundException {
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
        if (HDFSUtils.isHdfs(fileName)) return false;
        if (fileName.toLowerCase().startsWith("file:")) return true;
        return true;
    }

    public static PrintWriter getPrintWriter(String fileName, Writer out) throws IOException {
        if (fileName == null) return null;
        Writer writer;
        
        if (HDFSUtils.isHdfs(fileName)) {
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
}
