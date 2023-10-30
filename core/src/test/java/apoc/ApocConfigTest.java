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

import java.io.File;
import java.util.Collections;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.internal.SimpleLogService;
import org.neo4j.procedure.impl.GlobalProceduresRegistry;

import static apoc.ApocConfig.APOC_MAX_DECOMPRESSION_RATIO;
import static apoc.ApocConfig.SUN_JAVA_COMMAND;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ApocConfigTest {

    private ApocConfig apocConfig;
    private File apocConfigFile;
    @Before
    public void setup() throws Exception {
        LogProvider logProvider = new AssertableLogProvider();

        Config neo4jConfig = mock(Config.class);
        when(neo4jConfig.getDeclaredSettings()).thenReturn(Collections.emptyMap());
        when(neo4jConfig.get( any())).thenReturn(null);
        when(neo4jConfig.get(GraphDatabaseSettings.allow_file_urls)).thenReturn(false);

        apocConfigFile = new File(getClass().getClassLoader().getResource("apoc.conf").toURI());

        GlobalProceduresRegistry registry = mock(GlobalProceduresRegistry.class);
        DatabaseManagementService databaseManagementService = mock(DatabaseManagementService.class);
        apocConfig = new ApocConfig(neo4jConfig, new SimpleLogService(logProvider), registry, databaseManagementService);
    }

    private void setApocConfigSystemProperty() {
        System.setProperty(
                SUN_JAVA_COMMAND,
                "com.neo4j.server.enterprise.CommercialEntryPoint --home-dir=/home/stefan/neo4j-enterprise-4.0.0-alpha09mr02 --config-dir=" +  apocConfigFile.getParent()
        );
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
        setApocConfigSystemProperty();
        apocConfig.init();

        assertEquals("bar", apocConfig.getConfig().getString("foo"));
    }

    @Test
    public void testDetermineNeo4jConfFolderWithWhitespaces() {
        System.setProperty(SUN_JAVA_COMMAND, "com.neo4j.server.enterprise.CommercialEntryPoint --config-dir=/home/stefan/neo4j enterprise-4.0.0-alpha09mr02/conf --home-dir=/home/stefan/neo4j enterprise-4.0.0-alpha09mr02");

        assertEquals("/home/stefan/neo4j enterprise-4.0.0-alpha09mr02/conf", apocConfig.determineNeo4jConfFolder());
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
        System.clearProperty(APOC_MAX_DECOMPRESSION_RATIO);
    }
}