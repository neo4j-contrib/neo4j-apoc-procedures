package apoc.util.google.cloud;

import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;

public class GCStorageURLStreamHandler extends URLStreamHandler {
    public GCStorageURLStreamHandler() {}

    @Override
    protected URLConnection openConnection(final URL url) throws IOException {
        return new GCStorageURLConnection(url);
    }
}
