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
package apoc.load;

import apoc.util.CompressionAlgo;
import apoc.util.JsonUtil;
import apoc.util.TestUtil;
import apoc.util.Util;
import com.sun.net.httpserver.HttpContext;
import com.sun.net.httpserver.HttpServer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.mockserver.client.MockServerClient;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.model.Header;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.Result;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static apoc.ApocConfig.APOC_IMPORT_FILE_ENABLED;
import static apoc.ApocConfig.APOC_IMPORT_FILE_USE_NEO4J_CONFIG;
import static apoc.ApocConfig.apocConfig;
import static apoc.util.BinaryTestUtil.fileToBinary;
import static apoc.util.CompressionConfig.COMPRESSION;
import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockserver.integration.ClientAndServer.startClientAndServer;
import static org.mockserver.matchers.Times.exactly;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

public class LoadJsonTest {

    private static ClientAndServer mockServer;
    private HttpServer server;

    @BeforeClass
    public static void startServer() {
        mockServer = startClientAndServer(1080);
    }

    @AfterClass
    public static void stopServer() {
        mockServer.stop();
    }

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(GraphDatabaseSettings.memory_tracking, true)
            .withSetting(GraphDatabaseInternalSettings.enable_experimental_cypher_versions, true);

    @Before
    public void setUp() throws IOException {
        apocConfig().setProperty(APOC_IMPORT_FILE_ENABLED, true);
        apocConfig().setProperty(APOC_IMPORT_FILE_USE_NEO4J_CONFIG, false);
        apocConfig().setProperty("apoc.json.zip.url", "http://localhost:5353/testload.zip?raw=true!person.json");
        apocConfig()
                .setProperty(
                        "apoc.json.simpleJson.url",
                        ClassLoader.getSystemResource("map.json").toString());
        TestUtil.registerProcedure(db, LoadJsonExtended.class);

        server = HttpServer.create(new InetSocketAddress(5353), 0);
        HttpContext staticContext = server.createContext("/");
        staticContext.setHandler(new SimpleHttpHandler());
        server.start();
    }

    @After
    public void cleanup() {
        server.stop(0);
        db.shutdown();
    }

    @Test
    public void testLoadMultiJsonWithBinary() {
        testResult(
                db,
                "CYPHER 25 CALL apoc.load.jsonParams($url, null, null, null, $config)",
                map(
                        "url",
                        fileToBinary(
                                new File(ClassLoader.getSystemResource("multi.json")
                                        .getPath()),
                                CompressionAlgo.FRAMED_SNAPPY.name()),
                        "config",
                        map(COMPRESSION, CompressionAlgo.FRAMED_SNAPPY.name())),
                this::commonAssertionsLoadJsonMulti);
    }

    private void commonAssertionsLoadJsonMulti(Result result) {
        Map<String, Object> row = result.next();
        assertEquals(map("foo", asList(1L, 2L, 3L)), row.get("value"));
        row = result.next();
        assertEquals(map("bar", asList(4L, 5L, 6L)), row.get("value"));
        assertFalse(result.hasNext());
    }

    @Test
    public void testLoadJsonParamsWithAuth() throws Exception {
        String userPass = "user:password";
        String token = Util.encodeUserColonPassToBase64(userPass);
        Map<String, String> responseBody = Map.of("result", "message");

        new MockServerClient("localhost", 1080)
                .when(
                        request()
                                .withMethod("POST")
                                .withPath("/docs/search")
                                .withHeader("Authorization", "Basic " + token)
                                .withHeader("Content-type", "application/json")
                                .withBody("{\"query\":\"pagecache\",\"version\":\"3.5\"}"),
                        exactly(1))
                .respond(response()
                        .withStatusCode(200)
                        .withHeaders(
                                new Header("Content-Type", "application/json"),
                                new Header("Cache-Control", "public, max-age=86400"))
                        .withBody(JsonUtil.OBJECT_MAPPER.writeValueAsString(responseBody))
                        .withDelay(TimeUnit.SECONDS, 1));

        testCall(
                db,
                "CYPHER 25 CALL apoc.load.jsonParams($url, $config, $payload)",
                map(
                        "payload",
                        "{\"query\":\"pagecache\",\"version\":\"3.5\"}",
                        "url",
                        "http://" + userPass + "@localhost:1080/docs/search",
                        "config",
                        map("method", "POST", "Content-Type", "application/json")),
                (row) -> assertEquals(responseBody, row.get("value")));
    }

    @Test
    public void testLoadJsonParams() {
        new MockServerClient("localhost", 1080)
                .when(
                        request()
                                .withMethod("POST")
                                .withPath("/docs/search")
                                .withHeader("Content-type", "application/json"),
                        exactly(1))
                .respond(response()
                        .withStatusCode(200)
                        .withHeaders(
                                new Header("Content-Type", "application/json"),
                                new Header("Cache-Control", "public, max-age=86400"))
                        .withBody("{ result: 'message' }")
                        .withDelay(TimeUnit.SECONDS, 1));

        testCall(
                db,
                "CYPHER 25 CALL apoc.load.jsonParams($url, $config, $json)",
                map(
                        "json",
                        "{\"query\":\"pagecache\",\"version\":\"3.5\"}",
                        "url",
                        "http://localhost:1080/docs/search",
                        "config",
                        map("method", "POST", "Content-Type", "application/json")),
                (row) -> {
                    Map<String, Object> value = (Map<String, Object>) row.get("value");
                    assertFalse("value should be not empty", value.isEmpty());
                });
    }
}
