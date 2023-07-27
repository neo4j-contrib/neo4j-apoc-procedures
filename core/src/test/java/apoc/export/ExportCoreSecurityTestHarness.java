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
package apoc.export;

import apoc.ApocConfig;
import apoc.export.csv.ExportCSV;
import apoc.export.cypher.ExportCypher;
import apoc.export.graphml.ExportGraphML;
import apoc.export.json.ExportJson;
import apoc.util.TestUtil;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import static apoc.export.SecurityTestUtil.EXPORT_PROCEDURES;

public class ExportCoreSecurityTestHarness
{
    protected static final File directory = new File("target/import");
    protected static final File directoryWithSamePrefix = new File("target/imported");
    protected static final File subDirectory = new File("target/import/tests");
    protected static final List<String> APOC_EXPORT_PROCEDURE_NAME = Arrays.asList("csv", "json", "graphml", "cypher");

    public static final String FILENAME = "my-test.txt";
    public static final String PARAM_NAMES = "Procedure: {0}.{1}, fileName: {3}";


    static {
        //noinspection ResultOfMethodCallIgnored
        directory.mkdirs();
        //noinspection ResultOfMethodCallIgnored
        subDirectory.mkdirs();
        //noinspection ResultOfMethodCallIgnored
        directoryWithSamePrefix.mkdirs();
    }

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(GraphDatabaseSettings.load_csv_file_url_root, directory.toPath().toAbsolutePath());

    @BeforeClass
    public static void setUp() {
        Logger logger = Logger.getLogger( ExportCoreSecurityTestHarness.class.getName());
        logger.setLevel(Level.SEVERE);

        TestUtil.registerProcedure(db, ExportCSV.class, ExportJson.class, ExportGraphML.class, ExportCypher.class);
    }

    public static void setFileExport(boolean allowed) {
        ApocConfig.apocConfig().setProperty(ApocConfig.APOC_EXPORT_FILE_ENABLED, allowed);
    }

    public static List<Object[]> getParameterData(List<Pair<String, Consumer<Map>>> fileAndErrors) {
        return getParameterData(fileAndErrors, EXPORT_PROCEDURES, APOC_EXPORT_PROCEDURE_NAME);
    }

    public static List<Object[]> getParameterData(List<Pair<String, Consumer<Map>>> fileAndErrors, List<Pair<String, String>> importAndLoadProcedures, List<String> procedureNames) {
        // from a stream of fileNames and a List of Pair<procName, procArgs>
        // returns a List of String[]{ procName, procArgs, fileName }
        return fileAndErrors.stream()
                .flatMap(fileName -> importAndLoadProcedures
                        .stream()
                        .flatMap(procPair -> procedureNames
                                .stream()
                                .map(procName ->
                                        new Object[] { procName, procPair.getLeft(), procPair.getRight(), fileName.getLeft(), fileName.getRight() }
                                )
                        )
                ).collect(Collectors.toList());
    }
}