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
package apoc.export.arrow;

import apoc.export.ExportCoreSecurityTest;
import apoc.export.SecurityTestUtil;
import apoc.meta.Meta;
import apoc.util.TestUtil;
import apoc.util.Util;
import com.nimbusds.jose.util.Pair;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.io.File;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static apoc.export.ExportCoreSecurityTest.FILENAME;
import static apoc.export.ExportCoreSecurityTest.PARAM_NAMES;
import static apoc.export.ExportCoreSecurityTest.TestIllegalExternalFSAccess.case07;
import static apoc.export.ExportCoreSecurityTest.TestIllegalExternalFSAccess.EXCEPTION_NOT_FOUND_CONSUMER;
import static apoc.export.ExportCoreSecurityTest.TestIllegalExternalFSAccess.dataPairs;
import static apoc.export.ExportCoreSecurityTest.TestPathTraversalIsNormalisedWithinDirectory.MAIN_DIR_CONSUMER;
import static apoc.export.ExportCoreSecurityTest.TestPathTraversalIsNormalisedWithinDirectory.SUB_DIR_CONSUMER;
import static apoc.export.ExportCoreSecurityTest.TestPathTraversalIsNormalisedWithinDirectory.mainDirCases;
import static apoc.export.ExportCoreSecurityTest.TestPathTraversalIsNormalisedWithinDirectory.subDirCases;
import static apoc.export.SecurityTestUtil.ERROR_KEY;
import static apoc.export.SecurityTestUtil.PROCEDURE_KEY;
import static apoc.export.SecurityTestUtil.getApocProcedure;
import static apoc.export.SecurityTestUtil.setExportFileApocConfigs;
import static apoc.export.arrow.ExportArrowService.EXPORT_TO_FILE_ARROW_ERROR;

@RunWith(Enclosed.class)
public class ExportArrowSecurityTest {
    public static final File directory = new File("target/import");
    public static final File directoryWithSamePrefix = new File("target/imported");
    public static final File subDirectory = new File("target/import/tests");
    public static final List<String> APOC_EXPORT_PROCEDURE_NAME = List.of("arrow");

    static {
        //noinspection ResultOfMethodCallIgnored
        directory.mkdirs();
        //noinspection ResultOfMethodCallIgnored
        subDirectory.mkdirs();
        //noinspection ResultOfMethodCallIgnored
        directoryWithSamePrefix.mkdirs();
    }

    public static List<Pair<String, String>> EXPORT_PROCEDURES = List.of(
            Pair.of("query", "$fileName, 'RETURN 1', {}"),
            Pair.of("all", "$fileName, {}"),
            Pair.of("graph", "$fileName, {nodes: [], relationships: []}, {}")
    );

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(GraphDatabaseSettings.load_csv_file_url_root, directory.toPath().toAbsolutePath());

    @BeforeClass
    public static void setUp() {
        Logger logger = Logger.getLogger(ExportArrowSecurityTest.class.getName());
        logger.setLevel(Level.SEVERE);

        TestUtil.registerProcedure(db, ExportArrow.class, Meta.class);
    }

    private static Collection<Object[]> getParameterData(List<Pair<String, Consumer<Map>>> fileAndErrors) {
        return ExportCoreSecurityTest.getParameterData(fileAndErrors, EXPORT_PROCEDURES, APOC_EXPORT_PROCEDURE_NAME);
    }

    /**
     * These test with `apoc.import.file.use_neo4j_config=true`
     * should fail because they access a directory which isn't the configured directory
     */
    @RunWith(Parameterized.class)
    public static class TestIllegalExternalFSAccess {
        private final String apocProcedure;
        private final String fileName;
        private final Consumer consumer;

        public TestIllegalExternalFSAccess(String exportMethod, String exportMethodType, String exportMethodArguments,
                                           String fileName,
                                           Consumer consumer) {
            this.apocProcedure = getApocProcedure(exportMethod, exportMethodType, exportMethodArguments);
            this.fileName = fileName;
            this.consumer = consumer;
        }

        @Parameterized.Parameters(name = PARAM_NAMES)
        public static Collection<Object[]> data() {
            return ExportArrowSecurityTest.getParameterData(dataPairs);
        }

        @Test
        public void testsWithExportDisabled() {
            SecurityTestUtil.testsWithExportDisabled(db, apocProcedure, fileName, EXPORT_TO_FILE_ARROW_ERROR);
        }

        @Test
        public void testIllegalExternalFSAccessExportWithExportAndUseNeo4jConfEnabled() {
            // all assertions with `apoc.export.file.enabled=true` and `apoc.import.file.use_neo4j_config=true`

            // apoc.import.file.allow_read_from_filesystem=false
            setExportFileApocConfigs(true, true, false);
            SecurityTestUtil.assertPathTraversalError(db, apocProcedure, Map.of("fileName", fileName), consumer);

            // apoc.import.file.allow_read_from_filesystem=true
            setExportFileApocConfigs(true, true, true);
            SecurityTestUtil.assertPathTraversalError(db, apocProcedure, Map.of("fileName", fileName), consumer);
        }

        @Test
        public void testWithUseNeo4jConfDisabled() {
            // all assertions with `apoc.export.file.enabled=true` and `apoc.import.file.use_neo4j_config=false`

            // apoc.import.file.allow_read_from_filesystem=true
            setExportFileApocConfigs(true, false, true);
            testWithUseNeo4jConfFalse();

            // apoc.import.file.allow_read_from_filesystem=false
            setExportFileApocConfigs(true, false, false);
            testWithUseNeo4jConfFalse();
        }

        private void testWithUseNeo4jConfFalse() {
            // with `apoc.import.file.use_neo4j_config=false` this file export could outside the project
            if (fileName.equals(case07)) {
                return;
            }

            try {
                assertPathTraversalWithoutErrors();
            } catch (QueryExecutionException e) {
                EXCEPTION_NOT_FOUND_CONSUMER.accept(
                        Util.map(ERROR_KEY, e, PROCEDURE_KEY, apocProcedure)
                );
            }
        }

        private void assertPathTraversalWithoutErrors() {
            SecurityTestUtil.assertPathTraversalWithoutErrors(db, apocProcedure, fileName, new File("../", FILENAME));
        }
    }

    /**
     * These tests normalize the path to be within the import directory (or subdirectory) and make the file there.
     * Some attempt to exit the directory.
     */
    @RunWith(Parameterized.class)
    public static class TestPathTraversalIsNormalisedWithinDirectory {

        private final String apocProcedure;
        private final String fileName;
        private final Consumer consumer;

        public TestPathTraversalIsNormalisedWithinDirectory(String exportMethod, String exportMethodType, String exportMethodArguments,
                                                            String fileName,
                                                            Consumer consumer) {
            this.apocProcedure = getApocProcedure(exportMethod, exportMethodType, exportMethodArguments);
            this.fileName = fileName;
            this.consumer = consumer;
        }


        @Parameterized.Parameters(name = PARAM_NAMES)
        public static Collection<Object[]> data() {
            List<Pair<String, Consumer<Map>>> collect = mainDirCases.stream().map(i -> Pair.of(i, MAIN_DIR_CONSUMER)).collect(Collectors.toList());
            List<Pair<String, Consumer<Map>>> collect2 = subDirCases.stream().map(i -> Pair.of(i, SUB_DIR_CONSUMER)).toList();
            collect.addAll(collect2);

            return ExportArrowSecurityTest.getParameterData(collect);
        }

        @Test
        public void testPathTraversal() {
            setExportFileApocConfigs(true, true, false);
            File dir = getDir();

            assertPathTraversalWithoutErrors(dir);
        }

        @Test
        public void testIllegalFSAccessExport() {
            SecurityTestUtil.testsWithExportDisabled(db, apocProcedure, fileName, EXPORT_TO_FILE_ARROW_ERROR);
        }

        @Test
        public void testWithUseNeo4jConfDisabled() {
            // all assertions with `apoc.export.file.enabled=true` and `apoc.import.file.use_neo4j_config=true`
            File dir = getDir();

            // apoc.import.file.allow_read_from_filesystem=false
            setExportFileApocConfigs(true, true, false);
            assertPathTraversalWithoutErrors(dir);

            // apoc.import.file.allow_read_from_filesystem=true
            setExportFileApocConfigs(true, true, true);
            assertPathTraversalWithoutErrors(dir);
        }

        private File getDir() {
            if (subDirCases.contains(fileName)) {
                return subDirectory;
            }
            return directory;
        }

        private void assertPathTraversalWithoutErrors(File directory) {
            File file = new File(directory.getAbsolutePath(), FILENAME);
            SecurityTestUtil.assertPathTraversalWithoutErrors(db, apocProcedure, fileName, file);
        }
    }
}