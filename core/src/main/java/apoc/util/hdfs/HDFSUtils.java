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
import java.net.URL;

public class HDFSUtils {

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

			@Override
			public String getName() {
				return fileName;
			}
		};
	}

	public static StreamConnection readFile(URL url) throws IOException {
		return readFile(url.toString());
	}
	
	public static OutputStream writeFile(String fileName) throws IOException {
		FileSystem hdfs = getFileSystem(fileName);
		Path file = getPath(fileName);
		return hdfs.create(file);
	}

	public static Path getPath(String fileName) {
		return new Path(URI.create(fileName));
	}

	public static FileSystem getFileSystem(String fileName) throws IOException {
		Configuration configuration = new Configuration();
		return FileSystem.get(URI.create(fileName), configuration);
	}
}
