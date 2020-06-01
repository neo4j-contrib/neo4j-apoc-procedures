package apoc.util;

import apoc.export.util.CountingInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.net.URLConnection;

/**
 * @author mh
 * @since 26.01.18
 */
public interface StreamConnection {
    InputStream getInputStream() throws IOException;
    String getEncoding();
    long getLength();
    default CountingInputStream toCountingInputStream() throws IOException {
        return new CountingInputStream(getInputStream(),getLength());
    }

    static class UrlStreamConnection implements StreamConnection {
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
    }
}
