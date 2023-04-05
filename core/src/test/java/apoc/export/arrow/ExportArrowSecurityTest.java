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
import apoc.meta.Meta;
import apoc.util.FileUtils;
import apoc.util.TestUtil;
import junit.framework.TestCase;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Result;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static apoc.export.ExportCoreSecurityTest.assertPathTraversalError;
import static apoc.export.ExportCoreSecurityTest.assertPathTraversalWithoutErrors;
import static apoc.export.ExportCoreSecurityTest.getApocProcedure;
import static apoc.export.ExportCoreSecurityTest.setFileExport;
import static apoc.util.TestUtil.assertError;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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

    private static Map<String, List<String>> getArguments(List<String> cases) {
        return Map.of(
                "query", cases.stream().map(
                        filePath -> filePath + ", \"RETURN 1\", {}"
                ).collect(Collectors.toList()),
                "all", cases.stream().map(
                        filePath -> filePath + ", {}"
                ).collect(Collectors.toList()),
                "graph", cases.stream().map(
                        filePath -> filePath + ", {nodes: [], relationships: []}, {}"
                ).collect(Collectors.toList())
        );
    }

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(GraphDatabaseSettings.load_csv_file_url_root, directory.toPath().toAbsolutePath());

    @BeforeClass
    public static void setUp() {
        TestUtil.registerProcedure(db, ExportArrow.class, Meta.class);
        setFileExport(false);
    }

    private static Collection<String[]> data(Map<String, List<String>> apocProcedureArguments) {
        return ExportCoreSecurityTest.data(apocProcedureArguments, APOC_EXPORT_PROCEDURE_NAME);
    }

    @RunWith(Parameterized.class)
    public static class TestIllegalFSAccess {
        private final String apocProcedure;

        public TestIllegalFSAccess(String exportMethod, String exportMethodType, String exportMethodArguments) {
            this.apocProcedure = getApocProcedure(exportMethod, exportMethodType, exportMethodArguments);
        }

        private static final Map<String, List<String>> APOC_PROCEDURE_ARGUMENTS = Map.of(
                "query", List.of(
                        "'./hello', \"RETURN 'hello' as key\", {}",
                        "'./hello', \"RETURN 'hello' as key\", {stream:true}",
                        "'  ', \"RETURN 'hello' as key\", {}"
                ),
                "all", List.of(
                        "'./hello', {}",
                        "'./hello', {stream:true}",
                        "'  ', {}"
                ),
                "graph", List.of(
                        "'./hello', {nodes: [], relationships: []}, {}",
                        "'./hello', {nodes: [], relationships: []}, {stream:true}",
                        "'  ', {nodes: [], relationships: []}, {}"
                )
        );

        @Parameterized.Parameters
        public static Collection<String[]> data() {
            return ExportArrowSecurityTest.data(APOC_PROCEDURE_ARGUMENTS);
        }

        @Test
        public void testIllegalFSAccessExport() {
            QueryExecutionException e = Assert.assertThrows(QueryExecutionException.class,
                    () -> TestUtil.testCall(db, "CALL " + apocProcedure, (r) -> {})
            );

            assertError(e, ExportArrowService.EXPORT_TO_FILE_ARROW_ERROR, RuntimeException.class, apocProcedure);
        }
    }

    @RunWith(Parameterized.class)
    public static class TestIllegalExternalFSAccess {
        private final String apocProcedure;

        public TestIllegalExternalFSAccess(String exportMethod, String exportMethodType, String exportMethodArguments) {
            this.apocProcedure = getApocProcedure(exportMethod, exportMethodType, exportMethodArguments);
        }

        private static final Map<String, List<String>> METHOD_ARGUMENTS = Map.of(
                "query", List.of(
                        "'../hello', \"RETURN 'hello' as key\", {}",
                        "'file:../hello', \"RETURN 'hello' as key\", {}"
                ),
                "all", List.of(
                        "'../hello', {}",
                        "'file:../hello', {}"
                ),
                "graph", List.of(
                        "'../hello', {nodes: [], relationships: []}, {}",
                        "'file:../hello', {nodes: [], relationships: []}, {}"
                )
        );

        @Parameterized.Parameters
        public static Collection<String[]> data() {
            return ExportArrowSecurityTest.data(METHOD_ARGUMENTS);
        }

        @Test
        public void testIllegalExternalFSAccessExport() {
            final String message = apocProcedure + " should throw an exception";
            try {
                setFileExport(true);
                db.executeTransactionally("CALL " + apocProcedure, Map.of(),
                        Result::resultAsString);
                fail(message);
            } catch (Exception e) {
                assertError(e, FileUtils.ACCESS_OUTSIDE_DIR_ERROR, IOException.class, apocProcedure);
            } finally {
                setFileExport(false);
            }
        }
    }

    @RunWith(Parameterized.class)
    public static class TestPathTraversalAccess {
        private final String apocProcedure;

        public TestPathTraversalAccess(String exportMethod, String exportMethodType, String exportMethodArguments) {
            this.apocProcedure = getApocProcedure(exportMethod, exportMethodType, exportMethodArguments);
        }

        private static final Map<String, List<String>> METHOD_ARGUMENTS = getArguments(
                ExportCoreSecurityTest.TestPathTraversalAccess.cases
        );

        @Parameterized.Parameters
        public static Collection<String[]> data() {
            return ExportArrowSecurityTest.data(METHOD_ARGUMENTS);
        }

        @Test
        public void testPathTraversal() {
            assertPathTraversalError(db, "CALL " + apocProcedure);
        }
    }

    /**
     * All of these will resolve to a local path after normalization which will point to
     * a non-existing directory in our import folder: /apoc. Causing them to error that is
     * not found. They all attempt to exit the import folder back to the apoc folder:
     * Directory Layout: .../apoc/core/target/import
     */
    @RunWith(Parameterized.class)
    public static class TestPathTraversalIsNormalised {
        private final String apocProcedure;

        public TestPathTraversalIsNormalised(String exportMethod, String exportMethodType, String exportMethodArguments) {
            this.apocProcedure = getApocProcedure(exportMethod, exportMethodType, exportMethodArguments);
        }

        private static final Map<String, List<String>> METHOD_ARGUMENTS = getArguments(
                ExportCoreSecurityTest.TestPathTraversalIsNormalised.cases
        );

        @Parameterized.Parameters
        public static Collection<String[]> data() {
            return ExportArrowSecurityTest.data(METHOD_ARGUMENTS);
        }

        @Test
        public void testPathTraversal() {
            assertPathTraversalError(db, "CALL " + apocProcedure,
                    e -> TestCase.assertTrue(e.getMessage().contains("apoc/test.txt (No such file or directory)"))
            );
        }
    }

    /**
     * These tests normalize the path to be within the import directory and make the file there.
     * Some attempt to exit the directory.
     * They result in a file name test.txt being created (and deleted after).
     */
    @RunWith(Parameterized.class)
    public static class TestPathTraversalIsNormalisedWithinDirectory {
        private final String apocProcedure;

        public TestPathTraversalIsNormalisedWithinDirectory(String exportMethod, String exportMethodType, String exportMethodArguments) {
            this.apocProcedure = getApocProcedure(exportMethod, exportMethodType, exportMethodArguments);
        }

        public static final List<String> cases = ExportCoreSecurityTest.TestPathTraversalIsNormalisedWithinDirectory.cases;

        private static final Map<String, List<String>> METHOD_ARGUMENTS = getArguments(cases);

        @Parameterized.Parameters
        public static Collection<String[]> data() {
            return ExportArrowSecurityTest.data(METHOD_ARGUMENTS);
        }

        @Test
        public void testPathTraversal() {
            assertPathTraversalWithoutErrors(db, apocProcedure, directory,
                    (r) -> assertTrue(((String) r.get("file")).contains("test.txt")));
        }
    }

    /*
      * These test cases attempt to access a directory with the same prefix as the import directory. This is design to
      * test "directoryName.startsWith" logic which is a common path traversal bug.
     \*
      * All these tests should fail because they access a directory which isn't the configured directory
     */
    @RunWith(Parameterized.class)
    public static class TestPathTraversalIsWithSimilarDirectoryName {
        private final String apocProcedure;

        public TestPathTraversalIsWithSimilarDirectoryName(String exportMethod, String exportMethodType, String exportMethodArguments) {
            this.apocProcedure = getApocProcedure(exportMethod, exportMethodType, exportMethodArguments);
        }

        private static final Map<String, List<String>> METHOD_ARGUMENTS = getArguments(
                ExportCoreSecurityTest.TestPathTraversalIsWithSimilarDirectoryName.cases
        );

        @Parameterized.Parameters
        public static Collection<String[]> data() {
            return ExportArrowSecurityTest.data(METHOD_ARGUMENTS);
        }

        @Test
        public void testPathTraversal() {
            assertPathTraversalError(db, "CALL " + apocProcedure);
        }
    }


    /**
     * These tests normalize the path to be within the import directory and step into a subdirectory
     * to make the file there.
     * Some attempt to exit the directory.
     * They result in a file name test.txt in the directory /tests being created (and deleted after).
     */
    @RunWith(Parameterized.class)
    public static class TestPathTraversAllowedWithinDirectory {
        private final String apocProcedure;

        public TestPathTraversAllowedWithinDirectory(String exportMethod, String exportMethodType, String exportMethodArguments) {
            this.apocProcedure = getApocProcedure(exportMethod, exportMethodType, exportMethodArguments);
        }

        private static final Map<String, List<String>> METHOD_ARGUMENTS = getArguments(
                ExportCoreSecurityTest.TestPathTraversAllowedWithinDirectory.cases
        );

        @Parameterized.Parameters
        public static Collection<String[]> data() {
            return ExportArrowSecurityTest.data(METHOD_ARGUMENTS);
        }

        @Test
        public void testPathTraversal() {
            assertPathTraversalWithoutErrors(db, apocProcedure, subDirectory,
                    (r) -> assertTrue(((String) r.get("file")).contains("tests/test.txt")));
        }
    }
}