package apoc.util.google.cloud;

import apoc.util.FileUtils;

import java.net.URLStreamHandler;
import java.net.URLStreamHandlerFactory;

public class GCStorageURLStreamHandlerFactory implements URLStreamHandlerFactory {

    public GCStorageURLStreamHandlerFactory() {}

    @Override
    public URLStreamHandler createURLStreamHandler(final String protocol) {
        return protocol.equalsIgnoreCase(FileUtils.GCS_PROTOCOL) ? new GCStorageURLStreamHandler() : null;
    }
}
