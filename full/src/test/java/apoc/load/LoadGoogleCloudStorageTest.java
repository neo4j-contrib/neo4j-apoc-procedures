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

import apoc.util.GoogleCloudStorageContainerExtension;
import apoc.util.TestUtil;
import org.junit.*;

import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import static apoc.load.LoadCsvTest.assertRow;
import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class LoadGoogleCloudStorageTest {

    public static GoogleCloudStorageContainerExtension gcs = new GoogleCloudStorageContainerExtension()
            .withMountedResourceFile("test.csv", "/folder/test.csv")
            .withMountedResourceFile("map.json", "/folder/map.json");

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    @BeforeClass
    public static void setUp() throws Exception {
        gcs.start();
        TestUtil.registerProcedure(db, LoadCsv.class, LoadJson.class);
    }

    @AfterClass
    public static void tearDown() {
        gcs.close();
        db.shutdown();
    }

    @Test
    public void testLoadCsv() {
        String url = gcsUrl("b/folder/o/test.csv?alt=media");

        testResult(db, "CALL apoc.load.csv($url)", map("url", url), (r) -> {
            assertRow(r, "Selma", "8", 0L);
            assertRow(r, "Rana", "11", 1L);
            assertRow(r, "Selina", "18", 2L);
            assertFalse("It should be the last record", r.hasNext());
        });
    }

    @Test
    public void testLoadJSON() {
        String url = gcsUrl("b/folder/o/map.json?alt=media");
        testCall(db, "CALL apoc.load.jsonArray($url, '$.foo')", map("url", url), (r) -> {
            assertEquals(asList(1L,2L,3L), r.get("value"));
        });
    }

    private String gcsUrl(String path) {
        return String.format("http://%s:%d/storage/v1/%s", gcs.getContainerIpAddress(), gcs.getMappedPort(4443), path);
    }
}
