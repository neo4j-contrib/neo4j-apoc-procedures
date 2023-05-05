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

import apoc.ApocSettings;
import apoc.util.TestUtil;
import apoc.util.Util;
import apoc.util.s3.S3Container;
import apoc.xml.XmlTestUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.driver.internal.util.Iterables;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import static apoc.load.LoadCsvTest.assertRow;
import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

public class LoadS3Test {

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(ApocSettings.apoc_import_file_use__neo4j__config, false)
            .withSetting(ApocSettings.apoc_import_file_enabled, true);

    private S3Container minio;

    @BeforeClass
    public static void init() {
        // In test environment we skip the MD5 validation that can cause issues
        System.setProperty("com.amazonaws.services.s3.disableGetObjectMD5Validation", "true");
        System.setProperty("com.amazonaws.sdk.disableCertChecking", "true");
    }

    @AfterClass
    public static void destroy() {
        System.clearProperty("com.amazonaws.services.s3.disableGetObjectMD5Validation");
        System.clearProperty("com.amazonaws.sdk.disableCertChecking");
    }

    @Before
    public void setUp() throws Exception {
        TestUtil.registerProcedure(db, LoadCsv.class, LoadJson.class, Xml.class);
        minio = new S3Container();
        assumeTrue(minio.isRunning());
    }

    @After
    public void tearDown() throws Exception {
        minio.close();
    }

    @Test
    public void testLoadCsvS3() throws Exception {
        String url = minio.putFile("src/test/resources/test.csv");
        testResult(db, "CALL apoc.load.csv($url,{failOnError:false})", map("url", url), (r) -> {
            assertRow(r, "Selma", "8", 0L);
            assertRow(r, "Rana", "11", 1L);
            assertRow(r, "Selina", "18", 2L);
            assertEquals(false, r.hasNext());
        });
    }

    @Test public void testLoadJsonS3() throws Exception {
        String url = minio.putFile("src/test/resources/map.json");

        testCall(db, "CALL apoc.load.json($url,'')",map("url", url),
                (row) -> {
                    assertEquals(map("foo",asList(1L,2L,3L)), row.get("value"));
                });
    }

    @Test public void testLoadXmlS3() throws Exception {
        String url = minio.putFile("src/test/resources/xml/books.xml");

        testCall(db, "CALL apoc.load.xml($url,'/catalog/book[title=\"Maeve Ascendant\"]/.',{failOnError:false}) yield value as result", Util.map("url", url), (r) -> {
            Object value = Iterables.single(r.values());
            Assert.assertEquals(XmlTestUtils.XML_XPATH_AS_NESTED_MAP, value);
        });
    }


}
