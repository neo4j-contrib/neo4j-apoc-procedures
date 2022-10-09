package apoc.util;

import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream;

import java.io.InputStream;

public enum ArchiveType {
    NONE(null, null),
    ZIP(ZipArchiveInputStream.class, CompressionAlgo.NONE),
    TAR(TarArchiveInputStream.class, CompressionAlgo.NONE),
    TAR_GZ(TarArchiveInputStream.class, CompressionAlgo.GZIP);

    private final Class<?> stream;
    private final CompressionAlgo algo;

    ArchiveType(Class<?> stream, CompressionAlgo algo) {
        this.stream = stream;
        this.algo = algo;
    }

    public static ArchiveType from(String urlAddress) {
        if (!urlAddress.contains("!")) {
            return NONE;
        }
        if (urlAddress.contains(".zip")) {
            return ZIP;
        } else if (urlAddress.contains(".tar.gz") || urlAddress.contains(".tgz")) {
            return TAR_GZ;
        } else if (urlAddress.contains(".tar")) {
            return TAR;
        }
        return NONE;
    }
    
    public ArchiveInputStream getInputStream(InputStream is) {
        try {
            final InputStream compressionStream = algo.getInputStream(is);
            return (ArchiveInputStream) stream.getConstructor(InputStream.class).newInstance(compressionStream);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public boolean isArchive() {
        return stream != null;
    }
}
