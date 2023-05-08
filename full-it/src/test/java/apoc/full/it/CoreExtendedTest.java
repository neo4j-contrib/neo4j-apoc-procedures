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
package apoc.full.it;

import apoc.util.Neo4jContainerExtension;
import apoc.util.TestContainerUtil;
import apoc.util.TestUtil;
import org.junit.Test;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static apoc.ApocConfig.APOC_IMPORT_FILE_ENABLED;
import static apoc.util.TestContainerUtil.createEnterpriseDB;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

/*
 This test is just to verify if the APOC are correctly deployed
 into a Neo4j instance without any startup issue.
 If you don't have docker installed it will fail, and you can simply ignore it.
 */
public class CoreExtendedTest {
    @Test
    public void checkForCoreAndExtended() {
        try (Neo4jContainerExtension neo4jContainer = createEnterpriseDB(List.of(TestContainerUtil.ApocPackage.FULL), !TestUtil.isRunningInCI())) {

            neo4jContainer.withNeo4jConfig("dbms.transaction.timeout", "60s")
                    .withNeo4jConfig(APOC_IMPORT_FILE_ENABLED, "true");

            neo4jContainer.start();

            assumeTrue("Neo4j Instance should be up-and-running", neo4jContainer.isRunning());

            Session session = neo4jContainer.getSession();
            int coreCount = session.run("CALL apoc.help('') YIELD core WHERE core = true RETURN count(*) AS count").peek().get("count").asInt();
            int extendedCount = session.run("CALL apoc.help('') YIELD core WHERE core = false RETURN count(*) AS count").peek().get("count").asInt();

            assertTrue(coreCount > 0);
            assertTrue(extendedCount > 0);

        } catch (Exception ex) {
            if (TestContainerUtil.isDockerImageAvailable(ex)) {
                ex.printStackTrace();
                fail("Should not have thrown exception when trying to start Neo4j: " + ex);
            }
        }
    }

    @Test
    public void matchesSpreadsheet() {
        try(Neo4jContainerExtension neo4jContainer = createEnterpriseDB(List.of(TestContainerUtil.ApocPackage.FULL), !TestUtil.isRunningInCI())) {

            neo4jContainer.withNeo4jConfig("dbms.transaction.timeout", "5s");

            neo4jContainer.start();

            assumeTrue("Neo4j Instance should be up-and-running", neo4jContainer.isRunning());

            Session session = neo4jContainer.getSession();

            Result result = session.run("load csv with headers from 'file:///apoc-core-extended.csv' AS row RETURN row.Name as Name, row.Decision AS Decision");

            Map<String, String> spreadsheet = new HashMap<>();
            List<Record> list = result.list();
            for (Record record : list) {
                spreadsheet.put(record.get("Name").asString(), record.get("Decision").asString());
            }

            Map<String, String> actual = new HashMap<>();
            Result apocHelpResult = session.run("CALL apoc.help('')");
            for (Record record : apocHelpResult.list()) {
                actual.put(record.get("name").toString(), record.get("core").asBoolean() ? "CORE" : "EXTENDED");
            }

            List<Map.Entry<String, String>> different = spreadsheet.entrySet().stream().filter(entry -> actual.containsKey(entry.getKey()) && !actual.get(entry.getKey()).equals(entry.getValue())).collect(Collectors.toList());

            assertEquals(different.toString(), 0, different.size());

        } catch (Exception ex) {
            if (TestContainerUtil.isDockerImageAvailable(ex)) {
                ex.printStackTrace();
                fail("Should not have thrown exception when trying to start Neo4j: " + ex);
            }
        }
    }
}
