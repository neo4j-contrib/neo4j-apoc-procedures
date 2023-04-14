/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
