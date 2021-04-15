package apoc.util;

import java.nio.charset.Charset;
import java.util.Collections;
import java.util.Map;


public class CompressionConfig {

    private final String compressionAlgo;
    private final Charset charset;

    public CompressionConfig(Map<String, Object> config) {
        if (config == null) config = Collections.emptyMap();
        this.compressionAlgo = (String) config.getOrDefault("compression", "GZIP");
        this.charset = Charset.forName((String) config.getOrDefault("charset", "UTF-8"));
    }

    public String getCompressionAlgo() {
        return compressionAlgo;
    }

    public Charset getCharset() {
        return charset;
    }
}
