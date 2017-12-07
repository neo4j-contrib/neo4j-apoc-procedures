package apoc.util.s3util;


import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;

public class S3UrlStreamHandler extends URLStreamHandler {

    @Override
    protected URLConnection openConnection(URL url) throws IOException {
        return new S3URLConnection(url);
    }
}
