package apoc.util.s3util;

import java.net.URLStreamHandler;
import java.net.URLStreamHandlerFactory;

public class S3UrlStreamHandlerFactory implements URLStreamHandlerFactory{

    private static final String S3_URI = "s3";

    @Override
    public URLStreamHandler createURLStreamHandler(String protocol) {

        if (S3_URI.equals(protocol.toLowerCase())) {
            return new S3UrlStreamHandler();
        }
        return null;
    }
}
