package apoc.export.util;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;

public class HDFSUtils {
	
	public static Pattern HDFS_PATTERN = Pattern.compile("^(hdfs:\\/\\/)(?:[^@\\/\\n]+@)?([^\\/\\n]+)");
	
	private static Class<?> conifiguration = null;
	private static Class<?> fileSystem = null;
	private static Class<?> path = null;

	private HDFSUtils() {}
	
	public static boolean isHdfs(String fileName) {
		Matcher matcher = HDFS_PATTERN.matcher(fileName);
    	return matcher.find();
    }
	
	public static Class<?> getConfiguration() throws Exception {
		if (conifiguration == null) {
			conifiguration = Class.forName("org.apache.hadoop.conf.Configuration");
		}
		return conifiguration;
	}
	
	public static void setConfProperty(Object confInstance, String key, String value) throws Exception {
		confInstance.getClass().getMethod("set", String.class, String.class)
			.invoke(confInstance, key, value);
	}
	
	public static Class<?> getFileSystem() throws Exception {
		if (fileSystem == null) {
			fileSystem = Class.forName("org.apache.hadoop.fs.FileSystem");
		}
		return fileSystem;
	}
	
	public static Class<?> getPath() throws Exception {
		if (path == null) {
			path = Class.forName("org.apache.hadoop.fs.Path");
		}
		return path;
	}
	
	public static Object invokeFileSystemGet(URI uri, Object conf) throws Exception {
		return getFileSystem()
				.getDeclaredMethod("get", URI.class, conf.getClass())
				.invoke(null, uri, conf);
	}
	
	public static OutputStream invokeFileSystemCreate(URI uri, Object conf, String path) throws Exception {
		Object fs = invokeFileSystemGet(uri, conf);
		return (OutputStream) fs.getClass().getDeclaredMethod("create", getPath())
				.invoke(fs, getPath().getConstructor(String.class).newInstance(path));
	}
	
	public static String getHDFSUri(String fileName) {
		Matcher matcher = HDFS_PATTERN.matcher(fileName);
    	if (!matcher.find()) {
    		new RuntimeException("Not valid hdfs url");
    	}
    	return matcher.group();
	}
	
	public static OutputStream invokeFileSystemCreate(String fileName) throws Exception {
		String hdfsUri = getHDFSUri(fileName);
        String path = fileName.replace(hdfsUri, StringUtils.EMPTY);
        Object fs = invokeFileSystemGet(URI.create(hdfsUri), getConfiguration().newInstance());
		return (OutputStream) fs.getClass().getSuperclass().getDeclaredMethod("create", getPath())
				.invoke(fs, getPath().getConstructor(String.class).newInstance(path));
	}
	
	public static InputStream invokeFileSystemOpen(String fileName) throws Exception {
		String hdfsUri = getHDFSUri(fileName);
		String path = fileName.replace(hdfsUri, StringUtils.EMPTY);
        Object fs = invokeFileSystemGet(URI.create(hdfsUri), getConfiguration().newInstance());
		return (InputStream) fs.getClass().getSuperclass().getDeclaredMethod("open", getPath())
				.invoke(fs, getPath().getConstructor(String.class).newInstance(path));
	}
}
