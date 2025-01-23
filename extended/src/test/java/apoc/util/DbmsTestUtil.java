package apoc.util;

import org.junit.rules.TemporaryFolder;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.neo4j.configuration.GraphDatabaseSettings.procedure_unrestricted;

public class DbmsTestUtil {

    public static DatabaseManagementService startDbWithApocConfigs(
            TemporaryFolder storeDir, 
            Map<String, Object> configMap) throws IOException {
        storeDir.newFolder("conf"); // add the conf folder
        final File configFile = storeDir.newFile("conf/apoc.conf");
        try (FileWriter writer = new FileWriter(configFile)) {
            // `key=value` lines in apoc.conf file
            String confString = configMap.entrySet()
                    .stream()
                    .map(e -> e.getKey() + "=" + e.getValue())
                    .collect(Collectors.joining("\n"));

            writer.write(confString);
        }

        return new TestDatabaseManagementServiceBuilder(storeDir.getRoot().toPath())
                .setConfig(procedure_unrestricted, List.of("apoc*"))
                .build();
    }

    public static TestDatabaseManagementServiceBuilder getDbBuilderWithApocConfigs(
            TemporaryFolder storeDir, 
            Map<String, Object> configMap) throws IOException {
        try {
            storeDir.newFolder("conf"); // add the conf folder
        } catch (IOException e) {
            // ignore - already exists
        }
        final File configFile = new File(storeDir.getRoot(),"conf/apoc.conf");
        try (FileWriter writer = new FileWriter(configFile)) {
            // `key=value` lines in apoc.conf file
            String confString = configMap.entrySet()
                    .stream()
                    .map(e -> e.getKey() + "=" + e.getValue())
                    .collect(Collectors.joining("\n"));

            writer.write(confString);
        }

        return new TestDatabaseManagementServiceBuilder(storeDir.getRoot().toPath())
                .setConfig(procedure_unrestricted, List.of("apoc*"));
    }
}
