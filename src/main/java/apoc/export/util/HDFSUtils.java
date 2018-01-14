package apoc.export.util;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.fs.Path;

public class HDFSUtils {
	
	public static Pattern HDFS_PATTERN = Pattern.compile("^(hdfs:\\/\\/)(?:[^@\\/\\n]+@)?([^\\/\\n]+)");
	
	private HDFSUtils() {}
	
	
	public static boolean isHdfs(String fileName) {
		Matcher matcher = HDFS_PATTERN.matcher(fileName);
    	return matcher.find();
    }
	
	public static InputStream readFile(String fileName) throws Exception {
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
		URL url = new URL(fileName);
		return url.openStream();
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
		Configuration configuration = new Configuration();
		FileSystem hdfs = FileSystem.get(new URI(hdfsUri), configuration);
		Path file = new Path(path);
		return hdfs.create(file);
	}
	
}
