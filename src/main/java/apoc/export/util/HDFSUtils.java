package apoc.export.util;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;
import java.net.URLStreamHandlerFactory;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;

public class HDFSUtils {
	
	public static Pattern HDFS_PATTERN = Pattern.compile("^(hdfs:\\/\\/)(?:[^@\\/\\n]+@)?([^\\/\\n]+)");
	
	private HDFSUtils() {}
	
	public static boolean isHdfs(String fileName) {
		Matcher matcher = HDFS_PATTERN.matcher(fileName);
    	return matcher.find();
    }
	
	public static InputStream readFile(String fileName) throws Exception {
		URL.setURLStreamHandlerFactory((URLStreamHandlerFactory) Class.forName("org.apache.hadoop.fs.FsUrlStreamHandlerFactory").newInstance());
		URL url = new URL(fileName);
		return url.openStream();
	}
	
	private static Class<?> getConfiguration() throws Exception {
		return Class.forName("org.apache.hadoop.conf.Configuration");
	}
	
	
	private static Class<?> getFileSystem() throws Exception {
		return Class.forName("org.apache.hadoop.fs.FileSystem");
	}
	
	private static Class<?> getPath() throws Exception {
		return Class.forName("org.apache.hadoop.fs.Path");
	}
	
	private static Object invokeFileSystemGet(URI uri, Object conf) throws Exception {
		return getFileSystem()
				.getDeclaredMethod("get", URI.class, conf.getClass())
				.invoke(null, uri, conf);
	}
	
	private static String getHDFSUri(String fileName) {
		Matcher matcher = HDFS_PATTERN.matcher(fileName);
    	if (!matcher.find()) {
    		new RuntimeException("Not valid hdfs url");
    	}
    	return matcher.group();
	}
	
	public static OutputStream writeFile(String fileName) throws Exception {
		String hdfsUri = getHDFSUri(fileName);
        String path = fileName.replace(hdfsUri, StringUtils.EMPTY);
        Object fs = invokeFileSystemGet(URI.create(hdfsUri), getConfiguration().newInstance());
		return (OutputStream) fs.getClass().getSuperclass().getDeclaredMethod("create", getPath())
				.invoke(fs, getPath().getConstructor(String.class).newInstance(path));
	}
	
}
