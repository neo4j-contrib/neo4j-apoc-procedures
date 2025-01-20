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
package apoc.full.it.azure;

import static apoc.ApocConfig.APOC_EXPORT_FILE_ENABLED;
import static apoc.ApocConfig.APOC_IMPORT_FILE_ENABLED;
import static apoc.ApocConfig.apocConfig;
import static apoc.export.arrow.ArrowTestUtil.initDbCommon;
import static apoc.export.arrow.ArrowTestUtil.testImportCommon;
import static apoc.export.arrow.ArrowTestUtil.testLoadArrow;

import apoc.export.arrow.ArrowTestUtil;
import java.util.Map;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

@Ignore(
        "This test won't work until the Azure Storage files will be correctly handled via FileUtils, placed in APOC Core")
public class ArrowAzureStorageTest extends AzureStorageBaseTest {

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule();

    @Before
    public void beforeClass() {
        initDbCommon(db);
        apocConfig().setProperty(APOC_IMPORT_FILE_ENABLED, true);
        apocConfig().setProperty(APOC_EXPORT_FILE_ENABLED, true);
    }

    @Test
    public void testFileRoundtripWithLoadArrow() {
        String url = putToAzureStorageAndGetUrl("test_all.arrow");

        String file = db.executeTransactionally(
                "CALL apoc.export.arrow.all($url) YIELD file", Map.of("url", url), ArrowTestUtil::extractFileName);

        // check that the exported file is correct
        final String query = "CALL apoc.load.arrow($file, {})";
        testLoadArrow(db, query, Map.of("file", file));
    }

    @Test
    public void testFileRoundtripWithImportArrow() {
        db.executeTransactionally("CREATE (:Another {foo:1, listInt: [1,2]}), (:Another {bar:'Sam'})");

        String url = putToAzureStorageAndGetUrl("test_all_import.arrow");
        String file = db.executeTransactionally(
                "CALL apoc.export.arrow.all($url) YIELD file", Map.of("url", url), ArrowTestUtil::extractFileName);

        // check that the exported file is correct
        testImportCommon(db, file, ArrowTestUtil.MAPPING_ALL);
    }
}
