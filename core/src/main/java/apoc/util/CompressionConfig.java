package apoc.util;

import java.nio.charset.Charset;
import java.util.Collections;
import java.util.Map;

public class CompressionConfig {
    enum CompressionAlgo {GZIP, BZIP2, DEFLATE, BLOCK_LZ4, FRAMED_SNAPPY}

    private final CompressionAlgo compressionAlgo;
    private final Charset charset;

    public CompressionConfig(Map<String, Object> config) {
        if (config == null) config = Collections.emptyMap();
        this.compressionAlgo = CompressionAlgo.valueOf((String) config.getOrDefault("compression", "GZIP"));
        this.charset = Charset.forName((String) config.getOrDefault("charset", "UTF-8"));
    }

    public CompressionAlgo getCompressionAlgo() {
        return compressionAlgo;
    }

    public Charset getCharset() {
        return charset;
    }
}
