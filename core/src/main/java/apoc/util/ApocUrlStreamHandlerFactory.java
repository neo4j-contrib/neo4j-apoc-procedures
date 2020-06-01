package apoc.util;

import java.net.URLStreamHandler;
import java.net.URLStreamHandlerFactory;

public class ApocUrlStreamHandlerFactory implements URLStreamHandlerFactory {

    private static URLStreamHandlerFactory s3StreamHandlerFactory = Util.createInstanceOrNull("apoc.util.s3.S3UrlStreamHandlerFactory");
    private static URLStreamHandlerFactory gsStreamHandlerFactory = Util.createInstanceOrNull("apoc.util.google.cloud.GCStorageURLStreamHandlerFactory");
    private static URLStreamHandlerFactory hdfsStreamHandlerFactory =  Util.createInstanceOrNull("org.apache.hadoop.fs.FsUrlStreamHandlerFactory");

    @Override
    public URLStreamHandler createURLStreamHandler(String protocol) {
        if (FileUtils.S3_ENABLED && FileUtils.S3_PROTOCOL.equalsIgnoreCase(protocol)) {
            return s3StreamHandlerFactory.createURLStreamHandler(protocol);
        }
        if (FileUtils.HDFS_ENABLED && FileUtils.HDFS_PROTOCOL.equalsIgnoreCase(protocol)) {
            return hdfsStreamHandlerFactory.createURLStreamHandler(protocol);
        }
        if (FileUtils.GCS_ENABLED && FileUtils.GCS_PROTOCOL.equalsIgnoreCase(protocol)) {
            return gsStreamHandlerFactory.createURLStreamHandler(protocol);
        }
        return null;
    }
}
