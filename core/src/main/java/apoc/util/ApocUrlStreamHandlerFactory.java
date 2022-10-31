package apoc.util;

import java.net.URLStreamHandler;
import java.net.spi.URLStreamHandlerProvider;

public class ApocUrlStreamHandlerFactory extends URLStreamHandlerProvider
{

    @Override
    public URLStreamHandler createURLStreamHandler(String protocol) {
        FileUtils.SupportedProtocols supportedProtocol = FileUtils.SupportedProtocols.of(protocol);
        return supportedProtocol == null ? null : supportedProtocol.createURLStreamHandler();
    }

}
