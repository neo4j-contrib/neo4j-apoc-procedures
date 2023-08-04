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
package apoc;

import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.internal.SimpleLogService;
import org.neo4j.procedure.impl.GlobalProceduresRegistry;
import org.testcontainers.shaded.com.google.common.collect.Sets;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Collections;
import java.util.Set;

import static apoc.ApocConfig.APOC_MAX_DECOMPRESSION_RATIO;
import static apoc.ApocConfig.SUN_JAVA_COMMAND;
import static java.nio.file.attribute.PosixFilePermission.GROUP_EXECUTE;
import static java.nio.file.attribute.PosixFilePermission.GROUP_READ;
import static java.nio.file.attribute.PosixFilePermission.GROUP_WRITE;
import static java.nio.file.attribute.PosixFilePermission.OTHERS_EXECUTE;
import static java.nio.file.attribute.PosixFilePermission.OTHERS_READ;
import static java.nio.file.attribute.PosixFilePermission.OTHERS_WRITE;
import static java.nio.file.attribute.PosixFilePermission.OWNER_EXECUTE;
import static java.nio.file.attribute.PosixFilePermission.OWNER_READ;
import static java.nio.file.attribute.PosixFilePermission.OWNER_WRITE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ApocConfigTest {

    private ApocConfig apocConfig;
    private File apocConfigFile;
    private static final Set<PosixFilePermission> permittedFilePermissionsForCommandExpansion =
            Set.of(OWNER_READ, OWNER_WRITE, GROUP_READ);
    private static final Set<PosixFilePermission> forbiddenFilePermissionsForCommandExpansion =
            Set.of(OWNER_EXECUTE, GROUP_WRITE, GROUP_EXECUTE, OTHERS_READ, OTHERS_WRITE, OTHERS_EXECUTE);

    @Before
    public void setup() throws Exception {
        LogProvider logProvider = new AssertableLogProvider();

        Config neo4jConfig = mock(Config.class);
        when(neo4jConfig.getDeclaredSettings()).thenReturn(Collections.emptyMap());
        when(neo4jConfig.get(any())).thenReturn(null);
        when(neo4jConfig.get(GraphDatabaseSettings.allow_file_urls)).thenReturn(false);
        when(neo4jConfig.expandCommands()).thenReturn(true);

        apocConfigFile = new File(getClass().getClassLoader().getResource("apoc.conf").toURI());
        Files.setPosixFilePermissions(apocConfigFile.toPath(), permittedFilePermissionsForCommandExpansion);

        GlobalProceduresRegistry registry = mock(GlobalProceduresRegistry.class);
        DatabaseManagementService databaseManagementService = mock(DatabaseManagementService.class);
        apocConfig = new ApocConfig(neo4jConfig, new SimpleLogService(logProvider), registry, databaseManagementService);
    }


    private void setApocConfigFilePermissions(Set<PosixFilePermission> forbidden) throws Exception {
        Files.setPosixFilePermissions(
                apocConfigFile.toPath(),
                Sets.union(permittedFilePermissionsForCommandExpansion, forbidden)
        );
    }

    private void setApocConfigSystemProperty() {
        System.setProperty(SUN_JAVA_COMMAND, "com.neo4j.server.enterprise.CommercialEntryPoint --home-dir=/home/stefan/neo4j-enterprise-4.0.0-alpha09mr02 --config-dir=" +  apocConfigFile.getParent());
    }

    @Test
    public void testDetermineNeo4jConfFolderDefault() {
        System.setProperty(SUN_JAVA_COMMAND, "");
        assertEquals(".", apocConfig.determineNeo4jConfFolder());
    }

    @Test
    public void testDetermineNeo4jConfFolder() {
        System.setProperty(SUN_JAVA_COMMAND, "com.neo4j.server.enterprise.CommercialEntryPoint --home-dir=/home/stefan/neo4j-enterprise-4.0.0-alpha09mr02 --config-dir=/home/stefan/neo4j-enterprise-4.0.0-alpha09mr02/conf");

        assertEquals("/home/stefan/neo4j-enterprise-4.0.0-alpha09mr02/conf", apocConfig.determineNeo4jConfFolder());
    }

    @Test
    public void testApocConfFileBeingLoaded() throws Exception {
        String confDir = new File(getClass().getClassLoader().getResource("apoc.conf").toURI()).getParent();
        System.setProperty(SUN_JAVA_COMMAND, "com.neo4j.server.enterprise.CommercialEntryPoint --home-dir=/home/stefan/neo4j-enterprise-4.0.0-alpha09mr02 --config-dir=" + confDir);
        apocConfig.init();

        assertEquals("bar", apocConfig.getConfig().getString("foo"));
    }

    @Test
    public void testDetermineNeo4jConfFolderWithWhitespaces() {
        System.setProperty(SUN_JAVA_COMMAND, "com.neo4j.server.enterprise.CommercialEntryPoint --config-dir=/home/stefan/neo4j enterprise-4.0.0-alpha09mr02/conf --home-dir=/home/stefan/neo4j enterprise-4.0.0-alpha09mr02");

        assertEquals("/home/stefan/neo4j enterprise-4.0.0-alpha09mr02/conf", apocConfig.determineNeo4jConfFolder());
    }

    @Test
    public void testApocConfWithExpandCommands() throws Exception {
        String validExpandLine = "command.expansion=$(echo \"expanded value\")";
        addLineToApocConfig(validExpandLine);
        setApocConfigSystemProperty();

        apocConfig.init();

        assertEquals("expanded value", apocConfig.getConfig().getString("command.expansion"));
        removeLineFromApocConfig(validExpandLine);
    }

    @Test
    public void testApocConfWithInvalidExpandCommands() throws Exception {
        String invalidExpandLine = "command.expansion.3=$(fakeCommand 3 + 3)";
        addLineToApocConfig(invalidExpandLine);
        setApocConfigSystemProperty();

        RuntimeException e = assertThrows(RuntimeException.class, apocConfig::init);
        String expectedMessage = "java.io.IOException: Cannot run program \"fakeCommand\": error=2, No such file or directory";
        Assertions.assertThat(e.getMessage()).contains(expectedMessage);

        removeLineFromApocConfig(invalidExpandLine);
    }

    @Test
    public void testApocConfWithWrongFilePermissions() throws Exception {
        for (PosixFilePermission filePermission : forbiddenFilePermissionsForCommandExpansion) {
            setApocConfigFilePermissions(Set.of(filePermission));
            setApocConfigSystemProperty();

            RuntimeException e = assertThrows(RuntimeException.class, apocConfig::init);
            String expectedMessage = "does not have the correct file permissions to evaluate commands.";
            Assertions.assertThat(e.getMessage()).contains(expectedMessage);
        }
        // Set back to permitted after test
        setApocConfigFilePermissions(permittedFilePermissionsForCommandExpansion);
    }

    @Test
    public void testApocConfWithoutExpandCommands() throws Exception {
        LogProvider logProvider = new AssertableLogProvider();

        Config neo4jConfig = mock(Config.class);
        when(neo4jConfig.getDeclaredSettings()).thenReturn(Collections.emptyMap());
        when(neo4jConfig.get( any())).thenReturn(null);
        when(neo4jConfig.expandCommands()).thenReturn(false);

        GlobalProceduresRegistry registry = mock(GlobalProceduresRegistry.class);
        DatabaseManagementService databaseManagementService = mock(DatabaseManagementService.class);
        ApocConfig apocConfig = new ApocConfig(neo4jConfig, new SimpleLogService(logProvider), registry, databaseManagementService);

        String validExpandLine = "command.expansion=$(echo \"expanded value\")";
        addLineToApocConfig(validExpandLine);
        setApocConfigSystemProperty();

        RuntimeException e = assertThrows(RuntimeException.class, apocConfig::init);
        String expectedMessage = "$(echo \"expanded value\") is a command, but config is not explicitly told to expand it. (Missing --expand-commands argument?)";
        Assertions.assertThat(e.getMessage()).contains(expectedMessage);

        removeLineFromApocConfig(validExpandLine);
    }

    @Test
    public void testMaxDecompressionRatioValidation() {

        LogProvider logProvider = new AssertableLogProvider();

        Config neo4jConfig = mock(Config.class);
        when(neo4jConfig.getDeclaredSettings()).thenReturn(Collections.emptyMap());
        when(neo4jConfig.get(any())).thenReturn(null);
        when(neo4jConfig.expandCommands()).thenReturn(false);

        GlobalProceduresRegistry registry = mock(GlobalProceduresRegistry.class);
        DatabaseManagementService databaseManagementService = mock(DatabaseManagementService.class);
        System.setProperty(APOC_MAX_DECOMPRESSION_RATIO, "0");
        ApocConfig apocConfig = new ApocConfig(neo4jConfig, new SimpleLogService(logProvider), registry, databaseManagementService);

        RuntimeException e = assertThrows(RuntimeException.class, apocConfig::init);
        String expectedMessage = String.format("value 0 is not allowed for the config option %s", APOC_MAX_DECOMPRESSION_RATIO);
        Assertions.assertThat(e.getMessage()).contains(expectedMessage);
    }

    private void removeLineFromApocConfig(String lineContent) throws IOException {
        File temp = new File("_temp_");
        PrintWriter out = new PrintWriter(new FileWriter(temp));
        Files.lines(apocConfigFile.toPath())
                .filter(line -> !line.contains(lineContent))
                .forEach(out::println);
        out.flush();
        out.close();
        temp.renameTo(apocConfigFile);
    }

    private void addLineToApocConfig(String line) throws IOException {
        FileWriter fw = new FileWriter(apocConfigFile,true);
        fw.write(line);
        fw.close();
    }

}
