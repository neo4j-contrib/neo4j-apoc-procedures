package apoc.util;

import org.junit.rules.TemporaryFolder;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import static apoc.ApocConfig.SUN_JAVA_COMMAND;
import static org.neo4j.configuration.GraphDatabaseSettings.procedure_unrestricted;

public class DbmsTestUtil {
    public static DatabaseManagementService startDbWithApocConfs(TemporaryFolder storeDir, String... conf) throws IOException {
        final File configFile = storeDir.newFile("apoc.conf");
        try (FileWriter writer = new FileWriter(configFile)) {
            writer.write(String.join("\n", conf));
        }
        System.setProperty(SUN_JAVA_COMMAND, "config-dir=" + storeDir.getRoot().getAbsolutePath());

        return new TestDatabaseManagementServiceBuilder(storeDir.getRoot().toPath())
                .setConfig(procedure_unrestricted, List.of("apoc*"))
                .build();
    }
}
