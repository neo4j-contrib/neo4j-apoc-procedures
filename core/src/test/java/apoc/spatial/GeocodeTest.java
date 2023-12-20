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
package apoc.spatial;

import static apoc.ApocConfig.apocConfig;
import static apoc.util.MapUtil.map;
import static org.junit.Assert.*;
import static org.junit.Assume.assumeTrue;
import static org.neo4j.test.assertion.Assert.assertEventually;

import apoc.util.JsonUtil;
import apoc.util.TestUtil;
import inet.ipaddr.IPAddressString;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.junit.*;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Result;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

public class GeocodeTest {

    private static final String BLOCKED_ADDRESS = "127.168.0.0";
    private static final String NON_BLOCKED_ADDRESS = "localhost";
    private static final String BLOCKED_ERROR = "access to /" + BLOCKED_ADDRESS
            + " is blocked via the configuration property unsupported.dbms.cypher_ip_blocklist";
    private static final String JAVA_NET_EXCEPTION = "Caused by: java.net";
    private static final String URL_FORMAT = "%s://%s/geocode/v1/json?q=PLACE&key=KEY";
    private static final String REVERSE_URL_FORMAT = "%s://%s/geocode/v1/json?q=LAT+LNG&key=KEY";

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(
                    GraphDatabaseInternalSettings.cypher_ip_blocklist, List.of(new IPAddressString(BLOCKED_ADDRESS)));

    @BeforeClass
    public static void initDb() {
        TestUtil.registerProcedure(db, Geocode.class);
    }

    @AfterClass
    public static void teardown() {
        db.shutdown();
    }

    // -- with config map
    @Test
    public void testWrongUrlButViaOtherProvider() throws Exception {
        // wrong url but doesn't fail because provider is osm, not opencage
        testGeocodeWithThrottling(
                "osm", false, map("url", "https://api.opencagedata.com/geocode/v1/json?q=PLACE&key=KEY111"));
    }

    @Test(expected = QueryExecutionException.class)
    public void testWrongUrlWithOpenCage() throws Exception {
        // overwrite ApocConfig provider
        testGeocodeWithThrottling(
                "osm",
                false,
                map("provider", "opencage", "url", "https://api.opencagedata.com/geocode/v1/json?q=PLACE&key=KEY111"));
    }

    // -- with apoc config

    @Test
    public void testGeocodeWithBlockedAddressWithApocConf() {
        final String geocodeBaseConfig = Geocode.PREFIX + ".opencage";
        apocConfig().setProperty(geocodeBaseConfig + ".key", "myKey");

        Stream.of("https", "http", "ftp").forEach(protocol -> {
            final String nonBlockedUrl = String.format(URL_FORMAT, protocol, NON_BLOCKED_ADDRESS);
            final String nonBlockedReverseUrl = String.format(REVERSE_URL_FORMAT, protocol, NON_BLOCKED_ADDRESS);

            final String geocodeConfigUrl = geocodeBaseConfig + ".url";
            final String geocodeConfigReverseUrl = geocodeBaseConfig + ".reverse.url";

            apocConfig().setProperty(geocodeConfigUrl, nonBlockedUrl);
            apocConfig()
                    .setProperty(geocodeConfigReverseUrl, String.format(REVERSE_URL_FORMAT, protocol, BLOCKED_ADDRESS));

            assertGeocodeBlockedUrl(true);

            apocConfig().setProperty(geocodeConfigUrl, String.format(URL_FORMAT, protocol, BLOCKED_ADDRESS));
            apocConfig().setProperty(geocodeConfigReverseUrl, nonBlockedReverseUrl);

            assertGeocodeBlockedUrl(false);

            apocConfig().setProperty(geocodeConfigUrl, nonBlockedUrl);
            apocConfig().setProperty(geocodeConfigReverseUrl, nonBlockedReverseUrl);

            assertGeocodeAllowedUrl(false);
            assertGeocodeAllowedUrl(true);
        });
    }

    @Test
    public void testGeocodeWithBlockedAddressWithConfigMap() {
        Stream.of("https", "http", "ftp").forEach(protocol -> {
            final String nonBlockedUrl = String.format(URL_FORMAT, protocol, NON_BLOCKED_ADDRESS);
            final String nonBlockedReverseUrl = String.format(REVERSE_URL_FORMAT, protocol, NON_BLOCKED_ADDRESS);

            assertGeocodeBlockedUrl(true, nonBlockedUrl, String.format(REVERSE_URL_FORMAT, protocol, BLOCKED_ADDRESS));

            assertGeocodeBlockedUrl(false, String.format(URL_FORMAT, protocol, BLOCKED_ADDRESS), nonBlockedReverseUrl);

            assertGeocodeAllowedUrl(false, nonBlockedUrl, nonBlockedReverseUrl);

            assertGeocodeAllowedUrl(true, nonBlockedUrl, nonBlockedReverseUrl);
        });
    }

    private void assertGeocodeBlockedUrl(boolean reverseGeocode) {
        assertGeocodeBlockedUrl(reverseGeocode, null, null);
    }

    private void assertGeocodeBlockedUrl(boolean reverseGeocode, String url, String reverseUrl) {
        // check that if either url or reverse address are blocked
        // respectively the apoc.spatial.geocode and the apoc.spatial.reverseGeocode procedure fails
        assertGeocodeFails(reverseGeocode, BLOCKED_ERROR, url, reverseUrl);
    }

    private void assertGeocodeAllowedUrl(boolean reverseGeocode) {
        assertGeocodeAllowedUrl(reverseGeocode, null, null);
    }

    private void assertGeocodeAllowedUrl(boolean reverseGeocode, String url, String reverseUrl) {
        // check that if neither url nor reverse url are blocked
        // the procedures continue the execution (in this case by throwing a `401` Exception)
        assertGeocodeFails(reverseGeocode, JAVA_NET_EXCEPTION, url, reverseUrl);
    }

    private void assertGeocodeFails(boolean reverseGeocode, String expectedMsgError, String url, String reverseUrl) {
        // url == null means that it is defined via apoc.conf
        Map<String, Object> conf = url == null
                ? Collections.emptyMap()
                : Map.of(
                        "key", "myOwnKey",
                        "url", url,
                        "reverseUrl", reverseUrl);

        assertGeocodeFails(reverseGeocode, expectedMsgError, conf);
    }

    private void assertGeocodeFails(boolean reverseGeocode, String expectedMsgError, Map<String, Object> conf) {
        QueryExecutionException e =
                assertThrows(QueryExecutionException.class, () -> testGeocode("opencage", 100, reverseGeocode, conf));

        final String actualMsgErr = e.getMessage();
        assertTrue("Actual err. message is " + actualMsgErr, actualMsgErr.contains(expectedMsgError));
    }

    @Test
    public void testGeocodeOSM() throws Exception {
        testGeocodeWithThrottling("osm", false);
    }

    @Test
    public void testReverseGeocodeOSM() throws Exception {
        testGeocodeWithThrottling("osm", true);
    }

    private void testGeocodeWithThrottling(String supplier, Boolean reverseGeocode) throws Exception {
        testGeocodeWithThrottling(supplier, reverseGeocode, Collections.emptyMap());
    }

    private void testGeocodeWithThrottling(String supplier, Boolean reverseGeocode, Map<String, Object> config)
            throws Exception {
        long fast = testGeocode(supplier, 100, reverseGeocode, config);
        long slow = testGeocode(supplier, 2000, reverseGeocode, config);
        assumeTrue(
                "Fast " + supplier + " took " + fast + "ms and slow took " + slow
                        + "ms, but expected slow to be at least twice as long",
                (1.0 * slow / fast) > 1.2);
    }

    private long testGeocode(String provider, long throttle, boolean reverseGeocode, Map<String, Object> config)
            throws Exception {
        setupSupplier(provider, throttle);
        AtomicLong time = new AtomicLong();

        if (reverseGeocode) {
            testReverseGeocodeAddress(time, config);
        } else {
            testGeocodeAddress((String) config.getOrDefault("provider", provider), time, config);
        }

        return time.get();
    }

    private void testReverseGeocodeAddress(AtomicLong time, Map<String, Object> config) throws Exception {
        InputStream is = getClass().getResourceAsStream("/spatial.json");
        Map<String, List<Object>> fileAsMap = JsonUtil.OBJECT_MAPPER.readValue(is, Map.class);
        List<Object> spatialEventValues = fileAsMap.get("events");

        for (Object address : spatialEventValues) {
            String query = "CALL apoc.spatial.reverseGeocode($latitude, $longitude, false, $config)";
            Map<String, Object> params = Map.of(
                    "latitude", ((Map) address).get("lat"), "longitude", ((Map) address).get("lon"), "config", config);
            waitForServerResponseOK(query, params, time, (res) -> {
                assertTrue(res.hasNext());
                res.forEachRemaining((r) -> {
                    assertNotNull(r.get("description"));
                    assertNotNull(r.get("location"));
                    assertNotNull(r.get("data"));
                });
            });
        }
    }

    private void setupSupplier(String providerName, long throttle) {
        apocConfig().setProperty(Geocode.PREFIX + ".provider", providerName);
        apocConfig().setProperty(Geocode.PREFIX + "." + providerName + ".throttle", Long.toString(throttle));
    }

    private void testGeocodeAddress(String provider, AtomicLong time, Map<String, Object> config) {
        String geocodeQuery = "CALL apoc.spatial.geocode($url,0)";

        // Test basic case returns results
        waitForServerResponseOK(
                "CALL apoc.spatial.geocode('FRANCE', 1, true, $config)",
                Map.of("config", config),
                time,
                (res) -> res.forEachRemaining((r) -> {
                    assertNotNull(r.get("description"));
                    assertNotNull(r.get("location"));
                    assertNotNull(r.get("data"));
                }));

        // Test fake addresses give no results
        List<String> fakeAddresses = List.of("this place should really not exist", "");
        for (String fakeAddress : fakeAddresses) {
            waitForServerResponseOK(geocodeQuery, map("url", fakeAddress), time, (res) -> assertFalse(res.hasNext()));
        }

        // Test given provider contains correct count of results, only 1 France should be returned
        waitForServerResponseOK(geocodeQuery, map("url", "FRANCE"), time, (res) -> {
            long actual = Iterators.count(res);
            assertEquals(1, actual);
        });

        // Test returned latitude and longitude are as expected
        testGeocodeAddress("21 rue Paul Bellamy 44000 NANTES FRANCE", 47.2221667, -1.5566624, time, config);
        testGeocodeAddress("12 Rue Cubain 49000 Angers France", 47.4607430, -0.5453014, time, config);
        testGeocodeAddress(
                "Rämistrasse 71 8006 Zürich Switzerland",
                provider.equals("opencage") ? 47.00016 : 47.37457,
                provider.equals("opencage") ? 8.01427 : 8.54875,
                time,
                config);
    }

    private void testGeocodeAddress(
            String address, double lat, double lon, AtomicLong time, Map<String, Object> config) {
        String query = "CALL apoc.spatial.geocodeOnce($url, $config)";
        Map<String, Object> params = Map.of("url", address, "config", config);
        waitForServerResponseOK(query, params, time, (result) -> {
            if (result.hasNext()) {
                Map<String, Object> row = result.next();
                Map value = (Map) row.get("location");
                assertNotNull("location found", value);
                assertEquals(
                        "Incorrect latitude found",
                        lat,
                        Double.parseDouble(value.get("latitude").toString()),
                        0.1);
                assertEquals(
                        "Incorrect longitude found",
                        lon,
                        Double.parseDouble(value.get("longitude").toString()),
                        0.1);
                assertFalse(result.hasNext());
            }
        });
    }

    private void waitForServerResponseOK(
            String query, Map<String, Object> params, AtomicLong time, Consumer<Result> resultObjectFunction) {
        assertEventually(
                () -> {
                    try {
                        long start = System.currentTimeMillis();
                        db.executeTransactionally(query, params, res -> {
                            resultObjectFunction.accept(res);
                            return null;
                        });

                        if (time != null) {
                            time.addAndGet(System.currentTimeMillis() - start);
                        }
                        return true;
                    } catch (Exception e) {
                        String msg = e.getMessage();
                        if (msg.contains("Server returned HTTP response code") || msg.contains("connect timed out")) {
                            return false;
                        }
                        throw e;
                    }
                },
                (value) -> value,
                20L,
                TimeUnit.SECONDS);
    }
}
