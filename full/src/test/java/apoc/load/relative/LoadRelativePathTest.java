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
package apoc.load.relative;

import apoc.ApocSettings;
import apoc.load.LoadCsv;
import apoc.load.Xml;
import apoc.util.TestUtil;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.Result;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testResult;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class LoadRelativePathTest {

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(ApocSettings.apoc_import_file_enabled, true)
            .withSetting(GraphDatabaseSettings.allow_file_urls, true)
            .withSetting(GraphDatabaseSettings.load_csv_file_url_root, Path.of(RESOURCE.toURI()).getParent());

    public static final URL RESOURCE = LoadRelativePathTest.class.getClassLoader().getResource("test.csv");

    public LoadRelativePathTest() throws URISyntaxException {
    }

    @Before public void setUp() throws Exception {
        TestUtil.registerProcedure(db, LoadCsv.class, Xml.class);
    }

    //CSV
    @Test public void testLoadRelativePathCsv() {
        String url = "test.csv";
        testResult(db, "CALL apoc.load.csv($url,{results:['map','list','stringMap','strings']})", map("url",url), // 'file:test.csv'
                (r) -> {
                    assertRow(r,0L,"name","Selma","age","8");
                    assertRow(r,1L,"name","Rana","age","11");
                    assertRow(r,2L,"name","Selina","age","18");
                    assertFalse(r.hasNext());
                });
    }

    private static void assertRow(Result r, long lineNo, Object...data) {
        Map<String, Object> row = r.next();
        Map<String, Object> map = map(data);
        List<Object> values = new ArrayList<>(map.values());
        Map<String, Object> stringMap = new HashMap<>();
        stringMap.putAll(map);
        assertEquals(map, row.get("map"));
        assertEquals(values, row.get("list"));
        assertEquals(values, row.get("strings"));
        assertEquals(stringMap, row.get("stringMap"));
        assertEquals(lineNo, row.get("lineNo"));
    }

}
