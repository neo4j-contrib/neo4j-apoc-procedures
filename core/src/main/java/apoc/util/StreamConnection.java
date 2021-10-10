package apoc.util;

import apoc.export.util.CountingInputStream;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URLConnection;
import java.util.zip.DeflaterInputStream;
import java.util.zip.GZIPInputStream;

/**
 * @author mh
 * @since 26.01.18
 */
public interface StreamConnection {
    InputStream getInputStream() throws IOException;
    String getEncoding();
    long getLength();
    String getName();

    default CountingInputStream toCountingInputStream() throws IOException {
        if ("gzip".equals(getEncoding()) || getName().endsWith(".gz")) {
            return new CountingInputStream(new GZIPInputStream(getInputStream()), getLength());
        }
        if ("deflate".equals(getName())) {
            return new CountingInputStream(new DeflaterInputStream(getInputStream()), getLength());
        }
        return new CountingInputStream(getInputStream(), getLength());
    }

    class UrlStreamConnection implements StreamConnection {
        private final URLConnection con;

        public UrlStreamConnection(URLConnection con) {
            this.con = con;
        }

        @Override
        public InputStream getInputStream() throws IOException {
            return con.getInputStream();
        }

        @Override
        public String getEncoding() {
            return con.getContentEncoding();
        }

        @Override
        public long getLength() {
            return con.getContentLength();
        }

        @Override
        public String getName() {
            return con.getURL().toString();
        }
    }

    class FileStreamConnection implements StreamConnection {
        public static final String CANNOT_OPEN_FILE_FOR_READING = "Cannot open file %s for reading.";
        private final File file;

        public FileStreamConnection(File file) throws IOException {
            this.file = file;
            if (!file.exists() || !file.isFile() || !file.canRead()) {
                throw new IOException(String.format(CANNOT_OPEN_FILE_FOR_READING, file.getAbsolutePath()));
            }
        }

        public FileStreamConnection(URI fileName) throws IOException {
            this(new File(fileName));
        }

        public FileStreamConnection(String fileName) throws IOException {
            this(new File(fileName));
        }

        @Override
        public InputStream getInputStream() throws IOException {
            return FileUtils.openInputStream(file);
        }

        @Override
        public String getEncoding() {
            return "UTF-8";
        }

        @Override
        public long getLength() {
            return file.length();
        }

        @Override
        public String getName() {
            return file.getName();
        }
    }
}
