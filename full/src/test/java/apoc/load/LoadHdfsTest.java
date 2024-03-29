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

import static apoc.load.LoadCsvTest.assertRow;
import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testResult;
import static org.junit.Assert.assertEquals;

import apoc.ApocSettings;
import apoc.util.HdfsTestUtils;
import apoc.util.TestUtil;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

public class LoadHdfsTest {

    @Rule
    public DbmsRule db = new ImpermanentDbmsRule().withSetting(ApocSettings.apoc_import_file_enabled, true);

    private MiniDFSCluster miniDFSCluster;

    @Before
    public void setUp() throws Exception {
        TestUtil.registerProcedure(db, LoadCsv.class);
        miniDFSCluster = HdfsTestUtils.getLocalHDFSCluster();
        FileSystem fs = miniDFSCluster.getFileSystem();
        String fileName = "test.csv";
        Path file = new Path(fileName);
        try (OutputStream out = fs.create(file); ) {
            URL url = ClassLoader.getSystemResource(fileName);
            try (InputStream in = url.openStream(); ) {
                IOUtils.copy(in, out);
            }
        }
    }

    @After
    public void tearDown() {
        miniDFSCluster.shutdown();
        db.shutdown();
    }

    @Test
    public void testLoadCsvFromHDFS() throws Exception {
        testResult(
                db,
                "CALL apoc.load.csv($url,{results:['map','list','stringMap','strings']})",
                map(
                        "url",
                        String.format(
                                "%s/user/%s/%s",
                                miniDFSCluster.getURI().toString(),
                                System.getProperty("user.name"),
                                "test.csv")), // 'hdfs://localhost:12345/user/<sys_user_name>/test.csv'
                (r) -> {
                    assertRow(r, 0L, "name", "Selma", "age", "8");
                    assertRow(r, 1L, "name", "Rana", "age", "11");
                    assertRow(r, 2L, "name", "Selina", "age", "18");
                    assertEquals(false, r.hasNext());
                });
    }
}
