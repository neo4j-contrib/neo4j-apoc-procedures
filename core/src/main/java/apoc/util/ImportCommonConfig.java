package apoc.util;

import java.nio.charset.Charset;
import java.util.Collections;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;

public class ImportCommonConfig {
    private static String binary;
    private static Charset binaryCharset;

    public static Map<String, Object> fromCommon(Map<String, Object> config) {
        if (config == null) config = Collections.emptyMap();
        binary = (String) config.get("binary");
        binaryCharset = Charset.forName((String) config.getOrDefault("binaryCharset", UTF_8.name()));
        return config;
    }

    public String getBinary() {
        return binary;
    }

    public Charset getBinaryCharset() {
        return binaryCharset;
    }

    public boolean isFileUrl() {
        return binary == null;
    }
}
