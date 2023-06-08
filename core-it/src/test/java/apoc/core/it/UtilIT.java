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
package apoc.core.it;

import apoc.ApocConfig;
import apoc.util.Util;
import inet.ipaddr.IPAddressString;
import junit.framework.TestCase;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.junit.jupiter.api.AfterEach;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.testcontainers.containers.GenericContainer;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class UtilIT {
    private GenericContainer httpServer;

    private GenericContainer setUpServer(Config neo4jConfig, String redirectURL) {
        new ApocConfig(neo4jConfig);
        GenericContainer httpServer = new GenericContainer("alpine")
                .withCommand("/bin/sh", "-c", String.format("while true; do { echo -e 'HTTP/1.1 301 Moved Permanently\\r\\nLocation: %s'; echo ; } | nc -l -p 8000; done",
                        redirectURL))
                .withExposedPorts(8000);
        httpServer.start();
        Assume.assumeNotNull(httpServer);
        Assume.assumeTrue(httpServer.isRunning());
        return httpServer;
    }

    @AfterEach
    public void tearDown() {
        if (httpServer != null)
        {
            httpServer.stop();
        }
    }

    @Test
    public void redirectShouldWorkWhenProtocolNotChangesWithUrlLocation() throws IOException {
        httpServer = setUpServer(null, "http://www.google.com");
        // given
        String url = getServerUrl(httpServer);

        // when
        String page = IOUtils.toString( Util.openInputStream( url, null, null, null), Charset.forName( "UTF-8"));

        // then
        assertTrue(page.contains("<title>Google</title>"));
    }

    @Test
    public void redirectWithBlockedIPsWithUrlLocation() {
        List<IPAddressString> blockedIPs = List.of(new IPAddressString("127.168.0.1/8"));

        Config neo4jConfig = mock(Config.class);
        when(neo4jConfig.get(GraphDatabaseInternalSettings.cypher_ip_blocklist)).thenReturn(blockedIPs);

        httpServer = setUpServer(neo4jConfig, "http://127.168.0.1");
        String url = getServerUrl(httpServer);

        IOException e = Assert.assertThrows(IOException.class,
                () -> Util.openInputStream(url, null, null, null)
        );
        TestCase.assertTrue(e.getMessage().contains("access to /127.168.0.1 is blocked via the configuration property unsupported.dbms.cypher_ip_blocklist"));
    }

    @Test
    public void redirectWithProtocolUpgradeIsAllowed() throws IOException {
        List<IPAddressString> blockedIPs = List.of(new IPAddressString("127.168.0.1/8"));

        Config neo4jConfig = mock(Config.class);
        when(neo4jConfig.get(GraphDatabaseInternalSettings.cypher_ip_blocklist)).thenReturn(blockedIPs);

        httpServer = setUpServer(neo4jConfig, "https://www.google.com");
        String url = getServerUrl(httpServer);

        // when
        String page = IOUtils.toString(Util.openInputStream(url, null, null, null), Charset.forName("UTF-8"));
        // then
        assertTrue(page.contains("<title>Google</title>"));
    }

    @Test
    public void redirectWithProtocolDowngradeIsNotAllowed() throws IOException {
        HttpURLConnection mockCon = mock(HttpURLConnection.class);
        when(mockCon.getResponseCode()).thenReturn(302);
        when(mockCon.getHeaderField("Location")).thenReturn("http://127.168.0.1");
        when(mockCon.getURL()).thenReturn(new URL("https://127.0.0.0"));

        RuntimeException e = Assert.assertThrows(RuntimeException.class,
                () -> Util.isRedirect(mockCon)
        );

        TestCase.assertTrue(e.getMessage().contains("The redirect URI has a different protocol: http://127.168.0.1"));
    }

    @Test
    public void shouldFailForExceedingRedirectLimit() {
        Config neo4jConfig = mock(Config.class);

        httpServer = setUpServer(neo4jConfig, "https://127.0.0.0");
        String url = getServerUrl(httpServer);

        ArrayList<GenericContainer> servers = new ArrayList<>();
        for (int i = 1; i <= 10; i++) {
            GenericContainer server = setUpServer(neo4jConfig, url);
            servers.add(server);
            url = getServerUrl(server);
        }

        String finalUrl = url;
        IOException e = Assert.assertThrows(IOException.class,
                () -> Util.openInputStream(finalUrl, null, null, null)
        );

        TestCase.assertTrue(e.getMessage().contains("Redirect limit exceeded"));

        for (GenericContainer server : servers) {
            server.stop();
        }
    }

    @Test(expected = RuntimeException.class)
    public void redirectShouldThrowExceptionWhenProtocolChangesWithFileLocation() throws IOException {
        try {
            httpServer = setUpServer(null, "file:/etc/passwd");
            // given
            String url = getServerUrl(httpServer);

            // when
            Util.openInputStream(url, null, null, null);
        } catch (RuntimeException e) {
            // then
            assertEquals("The redirect URI has a different protocol: file:/etc/passwd", e.getMessage());
            throw e;
        }
    }

    private String getServerUrl(GenericContainer httpServer) {
        return String.format("http://%s:%s", httpServer.getContainerIpAddress(), httpServer.getMappedPort(8000));
    }
}
