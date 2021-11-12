package apoc.util;

import java.net.URLStreamHandler;
import java.net.URLStreamHandlerFactory;

public class ApocUrlStreamHandlerFactory implements URLStreamHandlerFactory {

    @Override
    public URLStreamHandler createURLStreamHandler(String protocol) {
        FileUtils.SupportedProtocols supportedProtocol = FileUtils.SupportedProtocols.of(protocol);
        return supportedProtocol == null ? null : supportedProtocol.createURLStreamHandler();
    }
}
