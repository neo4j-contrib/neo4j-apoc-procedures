package apoc.util.google.cloud;

import apoc.util.FileUtils;

import java.net.URLStreamHandler;
import java.net.URLStreamHandlerFactory;

public class GCStorageURLStreamHandlerFactory implements URLStreamHandlerFactory {

    public GCStorageURLStreamHandlerFactory() {}

    @Override
    public URLStreamHandler createURLStreamHandler(final String protocol) {
        final FileUtils.SupportedProtocols supportedProtocols = FileUtils.SupportedProtocols.valueOf(protocol);
        return supportedProtocols == FileUtils.SupportedProtocols.gs ? new GCStorageURLStreamHandler() : null;
    }
}
