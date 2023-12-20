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
package apoc.core.it;

import static apoc.util.TestContainerUtil.createEnterpriseDB;
import static org.junit.Assert.assertEquals;

import apoc.util.Neo4jContainerExtension;
import apoc.util.TestContainerUtil;
import apoc.util.TestUtil;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.RepeatedTest;
import org.neo4j.driver.Session;

public class CollEnterpriseTest {

    private static Neo4jContainerExtension neo4jContainer;
    private static Session session;

    @BeforeAll
    public static void beforeAll() {
        // We build the project, the artifact will be placed into ./build/libs
        neo4jContainer = createEnterpriseDB(List.of(TestContainerUtil.ApocPackage.CORE), !TestUtil.isRunningInCI());
        neo4jContainer.start();
        session = neo4jContainer.getSession();
    }

    @AfterAll
    public static void afterAll() {
        session.close();
        neo4jContainer.close();
    }

    @RepeatedTest(50)
    public void testMin() {
        assertEquals(
                1L,
                session.run("RETURN apoc.coll.min([1,2]) as value")
                        .next()
                        .get("value")
                        .asLong());
        assertEquals(
                1L,
                session.run("RETURN apoc.coll.min([1,2,3]) as value")
                        .next()
                        .get("value")
                        .asLong());
        assertEquals(
                0.5D,
                session.run("RETURN apoc.coll.min([0.5,1,2.3]) as value")
                        .next()
                        .get("value")
                        .asDouble(),
                0.1);
    }

    @RepeatedTest(50)
    public void testMax() {
        assertEquals(
                3L,
                session.run("RETURN apoc.coll.max([1,2,3]) as value")
                        .next()
                        .get("value")
                        .asLong());
        assertEquals(
                2.3D,
                session.run("RETURN apoc.coll.max([0.5,1,2.3]) as value")
                        .next()
                        .get("value")
                        .asDouble(),
                0.1);
    }
}
