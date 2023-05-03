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
package apoc.util;

import apoc.config.Config;
import apoc.load.relative.LoadRelativePathTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.ProvideSystemProperty;
import org.junit.rules.TestName;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.net.URISyntaxException;
import java.nio.file.Paths;

import static apoc.ApocConfig.apocConfig;
import static org.junit.Assert.assertEquals;

public class FileUtilsTest {

    @Rule
    public TestName testName = new TestName();


    @Rule
    public DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(GraphDatabaseSettings.allow_file_urls, true);

    @Rule
    public ProvideSystemProperty sysprops = new ProvideSystemProperty("foo", "bar");

    private static String TEST_FILE_ABSOLUTE;

    static {
        try {
            TEST_FILE_ABSOLUTE = "file://" + Paths.get(LoadRelativePathTest.class.getClassLoader().getResource("./test.csv").toURI());
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    private static String TEST_FILE = LoadRelativePathTest.class.getClassLoader().getResource("test.csv").getPath();

    private static final String TEST_WITH_DIRECTORY_IMPORT = "WithDirectoryImport";
    private String TEST_FILE_RELATIVE;
    private String importFolder;

    @Before
    public void setUp() throws Exception {
        importFolder = db.databaseLayout().databaseDirectory().toFile().getAbsolutePath() + "/import/";
        TEST_FILE_RELATIVE = Paths.get(importFolder, "test.csv").toUri().toString();
        if (testName.getMethodName().endsWith(TEST_WITH_DIRECTORY_IMPORT)) {
            apocConfig().setProperty("dbms.directories.import", importFolder);
        }
        TestUtil.registerProcedure(db, Config.class);
    }

    @Test
    public void notChangeFileUrlWithDirectoryImportConstrainedURI() throws Exception {
        assertEquals(TEST_FILE_ABSOLUTE, FileUtils.changeFileUrlIfImportDirectoryConstrained(TEST_FILE));
    }

    @Test
    public void notChangeFileUrlWithDirectoryAndProtocolImportConstrainedURI() throws Exception {
        assertEquals(TEST_FILE_ABSOLUTE, FileUtils.changeFileUrlIfImportDirectoryConstrained("file://" + TEST_FILE));
    }

    @Test
    public void changeNoSlashesUrlWithDirectoryImportConstrainedURIWithDirectoryImport() throws Exception {
        assertEquals(TEST_FILE_RELATIVE, FileUtils.changeFileUrlIfImportDirectoryConstrained("test.csv"));
    }

    @Test
    public void changeSlashUrlWithDirectoryImportConstrainedURIWithDirectoryImport() throws Exception {
        assertEquals(TEST_FILE_RELATIVE, FileUtils.changeFileUrlIfImportDirectoryConstrained("/test.csv"));
    }

    @Test
    public void changeFileSlashUrlWithDirectoryImportConstrainedURIWithDirectoryImport() throws Exception {
        assertEquals(TEST_FILE_RELATIVE, FileUtils.changeFileUrlIfImportDirectoryConstrained("file:/test.csv"));
    }

    @Test
    public void changeFileDoubleSlashesUrlWithDirectoryImportConstrainedURIWithDirectoryImport() throws Exception {
        assertEquals(TEST_FILE_RELATIVE, FileUtils.changeFileUrlIfImportDirectoryConstrained("file://test.csv"));
    }

    @Test
    public void changeFileTripleSlashesUrlWithDirectoryImportConstrainedURIWithDirectoryImport() throws Exception {
        assertEquals(TEST_FILE_RELATIVE, FileUtils.changeFileUrlIfImportDirectoryConstrained("file:///test.csv"));
    }

    @Test
    public void importDirectoryWithRelativePathWithDirectoryImport() throws Exception {
        assertEquals(TEST_FILE_RELATIVE, FileUtils.changeFileUrlIfImportDirectoryConstrained("test.csv"));
    }


    @Test
    public void importDirectoryWithRelativeArchivePathWithDirectoryImport() throws Exception {
        String localPath = "test.zip!sub/test.csv";
        String expected = importFolder + "/" + localPath;
        assertEquals(Paths.get(expected).toUri().toString(), FileUtils.changeFileUrlIfImportDirectoryConstrained(localPath));
    }

}
