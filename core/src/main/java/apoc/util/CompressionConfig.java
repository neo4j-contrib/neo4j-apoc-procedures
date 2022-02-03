package apoc.util;

import java.nio.charset.Charset;
import java.util.Collections;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;

public class CompressionConfig {
    public static final String COMPRESSION = "compression";
    public static final String CHARSET = "charset";

    private final String compressionAlgo;
    private final Charset charset;

    public CompressionConfig(Map<String, Object> config) {
        if (config == null) config = Collections.emptyMap();
        this.compressionAlgo = (String) config.getOrDefault(COMPRESSION, CompressionAlgo.GZIP.name());
        this.charset = Charset.forName((String) config.getOrDefault(CHARSET, UTF_8.name()));
    }

    public String getCompressionAlgo() {
        return compressionAlgo;
    }

    public Charset getCharset() {
        return charset;
    }
}
