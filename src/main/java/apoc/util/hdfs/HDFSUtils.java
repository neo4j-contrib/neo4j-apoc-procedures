package apoc.util.hdfs;

import apoc.util.StreamConnection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HDFSUtils {

	public static Pattern HDFS_PATTERN = Pattern.compile("^(hdfs:\\/\\/)(?:[^@\\/\\n]+@)?([^\\/\\n]+)");

	private HDFSUtils() {}

	public static StreamConnection readFile(String fileName) throws IOException {
		FileSystem hdfs = getFileSystem(fileName);
		Path file = getPath(fileName);
		FileStatus fileStatus = hdfs.getFileStatus(file);
		return new StreamConnection() {
			@Override
			public InputStream getInputStream() throws IOException {
				return hdfs.open(file);
			}

			@Override
			public String getEncoding() {
				return "";
			}

			@Override
			public long getLength() {
				return fileStatus.getLen();
			}
		};
	}

	public static StreamConnection readFile(URL url) throws IOException {
		return readFile(url.toString());
	}
	
	private static String getHDFSUri(String fileName) {
		Matcher matcher = HDFS_PATTERN.matcher(fileName);
    	if (!matcher.find()) {
    		throw new RuntimeException("Not valid HDFS url");
    	}
    	return matcher.group();
	}
	
	public static OutputStream writeFile(String fileName) throws IOException {
		FileSystem hdfs = getFileSystem(fileName);
		Path file = getPath(fileName);
		return hdfs.create(file);
	}

	public static Path getPath(String fileName) {
		String path = fileName.replace(getHDFSUri(fileName), "");
		return new Path(path);
	}

	public static FileSystem getFileSystem(String fileName) throws IOException {
		String hdfsUri = getHDFSUri(fileName);
		Configuration configuration = new Configuration();
		return FileSystem.get(toUri(hdfsUri), configuration);
	}

	public static URI toUri(String url) {
		try {
			return new URI(url);
		} catch (URISyntaxException e) {
			throw new RuntimeException(e.getMessage(), e);
		}
	}

	public static boolean isHdfs(String fileName) {
        Matcher matcher = HDFS_PATTERN.matcher(fileName);
        return matcher.find();
    }
}
