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
package apoc.log;

import apoc.util.TestUtil;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

import java.nio.file.Paths;
import java.util.UUID;
import java.util.stream.Collectors;

import static apoc.ApocConfig.apocConfig;
import static apoc.util.TestUtil.testResult;
import static org.junit.Assert.assertTrue;

public class Neo4jLogStreamTest {
    
    private GraphDatabaseService db;

    @Before
    public void setUp() throws Exception {
        DatabaseManagementService dbManagementService = new TestDatabaseManagementServiceBuilder(
                Paths.get("target", UUID.randomUUID().toString()).toAbsolutePath()).build();
        apocConfig().setProperty("dbms.directories.logs", "");
        db = dbManagementService.database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME);
        TestUtil.registerProcedure(db, Neo4jLogStream.class);
    }
    
    @Test
    public void testLogStream() {
        testResult(db, "CALL apoc.log.stream('debug.log')", res -> {
            final String wholeFile = Iterators.stream(res.<String>columnAs("line")).collect(Collectors.joining(""));
            assertTrue(wholeFile.contains("apoc.import.file.enabled=false"));
        });
    }
}
