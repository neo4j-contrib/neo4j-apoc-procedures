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
package apoc.it.core;

import apoc.util.Neo4jContainerExtension;
import apoc.util.TestContainerUtil;
import apoc.util.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

import org.neo4j.driver.Session;

import static apoc.refactor.GraphRefactoringTest.CLONE_NODES_QUERY;
import static apoc.refactor.GraphRefactoringTest.CLONE_SUBGRAPH_QUERY;
import static apoc.refactor.GraphRefactoringTest.EXTRACT_QUERY;
import static apoc.util.TestContainerUtil.createEnterpriseDB;
import static apoc.util.TestContainerUtil.testCall;
import static apoc.util.TestUtil.isRunningInCI;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeNotNull;
import static org.junit.Assume.assumeTrue;

public class GraphRefactoringEnterpriseTest {
    private static final String CREATE_REL_FOR_EXTRACT_NODE = "CREATE (:Start)-[r:TO_MOVE {name: 'foobar', surname: 'baz'}]->(:End)";
    private static final String DELETE_REL_FOR_EXTRACT_NODE = "MATCH p=(:Start)-[r:TO_MOVE]->(:End) DELETE p";
    private static Neo4jContainerExtension neo4jContainer;
    private static Session session;

    @BeforeClass
    public static void beforeAll() {
        assumeFalse(isRunningInCI());
        TestUtil.ignoreException(() -> {
            neo4jContainer = createEnterpriseDB(List.of(TestContainerUtil.ApocPackage.CORE), true);
            neo4jContainer.start();
        }, Exception.class);
        assumeNotNull(neo4jContainer);
        assumeTrue("Neo4j Instance should be up-and-running", neo4jContainer.isRunning());
        session = neo4jContainer.getSession();
    }

    @AfterClass
    public static void afterAll() {
        if (neo4jContainer != null && neo4jContainer.isRunning()) {
            session.close();
            neo4jContainer.close();
        }
    }
    
    @Test
    public void testCloneNodesWithNodeKeyConstraint() {
        nodeKeyCommons(CLONE_NODES_QUERY);
    }

    @Test
    public void testCloneNodesWithBothExistenceAndUniqueConstraint() {
        uniqueNotNullCommons(CLONE_NODES_QUERY);
    }
    
    @Test
    public void testCloneSubgraphWithNodeKeyConstraint() {
        nodeKeyCommons(CLONE_SUBGRAPH_QUERY);
    }

    @Test
    public void testCloneSubgraphWithBothExistenceAndUniqueConstraint() {
        uniqueNotNullCommons(CLONE_SUBGRAPH_QUERY);
    }
    
    @Test
    public void testExtractNodesWithNodeKeyConstraint() {
        session.writeTransaction(tx -> tx.run(CREATE_REL_FOR_EXTRACT_NODE));
        nodeKeyCommons(EXTRACT_QUERY);
        session.writeTransaction(tx -> tx.run(DELETE_REL_FOR_EXTRACT_NODE));
    }

    @Test
    public void testExtractNodesWithBothExistenceAndUniqueConstraint() {
        session.writeTransaction(tx -> tx.run(CREATE_REL_FOR_EXTRACT_NODE));
        uniqueNotNullCommons(EXTRACT_QUERY);
        session.writeTransaction(tx -> tx.run(DELETE_REL_FOR_EXTRACT_NODE));
    }

    private void nodeKeyCommons(String query) {
        session.writeTransaction(tx -> tx.run("CREATE CONSTRAINT nodeKey ON (p:MyBook) ASSERT (p.name, p.surname) IS NODE KEY"));
        cloneNodesAssertions(query, "already exists with label `MyBook` and properties `name` = 'foobar', `surname` = 'baz'");
        session.writeTransaction(tx -> tx.run("DROP CONSTRAINT nodeKey"));
        
    }

    private void uniqueNotNullCommons(String query) {
        session.writeTransaction(tx -> tx.run("CREATE CONSTRAINT unique ON (p:MyBook) ASSERT (p.name) IS UNIQUE"));
        session.writeTransaction(tx -> tx.run("CREATE CONSTRAINT notNull ON (p:MyBook) ASSERT (p.name) IS NOT NULL"));

        cloneNodesAssertions(query, "already exists with label `MyBook` and property `name` = 'foobar'");
        session.writeTransaction(tx -> tx.run("DROP CONSTRAINT unique"));
        session.writeTransaction(tx -> tx.run("DROP CONSTRAINT notNull"));
    }

    private void cloneNodesAssertions(String query, String message) {
        session.writeTransaction(tx -> tx.run("CREATE (n:MyBook {name: 'foobar', surname: 'baz'})"));
        testCall(session, query,
                r -> {
                    final String error = (String) r.get("error");
                    assertTrue(error.contains(message));
                    assertNull(r.get("output"));
                    
                });
        session.writeTransaction(tx -> tx.run("MATCH (n:MyBook) DELETE n"));
    }
}
