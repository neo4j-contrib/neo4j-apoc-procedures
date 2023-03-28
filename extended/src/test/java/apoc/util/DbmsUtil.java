package apoc.util;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.stream.Collectors;

import static apoc.ApocConfig.SUN_JAVA_COMMAND;
import static apoc.ExtendedApocConfig.CONFIG_DIR;

public class DbmsUtil {

    public static void setApocConfigs(File confDir, Map<String, String> configMap) {
        final File conf = new File(confDir, "apoc.conf");
        try (FileWriter writer = new FileWriter(conf)) {
            // key=value in apoc.conf file
            String confString = configMap.entrySet().stream()
                    .map(e -> e.getKey() + "=" + e.getValue())
                    .collect(Collectors.joining("\n"));

            writer.write(confString);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        System.setProperty(SUN_JAVA_COMMAND, CONFIG_DIR + confDir.getAbsolutePath());
    }
}
